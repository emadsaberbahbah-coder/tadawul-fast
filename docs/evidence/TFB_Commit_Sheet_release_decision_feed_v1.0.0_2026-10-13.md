# TFB Commit Sheet - release_decision_feed v1.0.0 (P0-1, review blocker 1, owner decision D1)

Date: 2026-10-13. Base: origin/main dda1211. Lane: GitHub Actions plus one new offline-testable script. Gate: TFB_DECISION_RELEASE (default off) and TFB_DECISION_RELEASE_KILL (default off).

## Why this item

Review blocker 1 says the decision feed can never return to EXECUTABLE. That is confirmed on source. Since #751 the 'TFB Decision Feed' cell in _Status L:M is downgrade-only: scripts/run_dashboard_sync.py writes the feed only when its composite verdict is NOT EXECUTABLE, never creates the key, and never replaces a value that starts with NOT_ACTIONABLE(final_publication). scripts/finalize_decision_publication.py writes only through publish_blocker, which refuses anything that does not start with 'NOT_ACTIONABLE(final_publication) | '. The sync header (v6.67.0) promises that a final-publication veto survives "until an explicit accepted release replaces it", but no release step was ever built. So even on the day every strict audit passes, the board stays withheld.

This commit builds that explicit release, and nothing else. It is necessary but not sufficient for a live EXECUTABLE board: the arming prerequisites listed under Operator steps (blocker 5 and four others) are still owed and are not built here. Until they land, every real release dry-run is expected to exit 2. That is correct behaviour, not a defect.

## Evidence

- run_dashboard_sync.py _write_upstream_verdict: the feed is upserted only on `if final_feed and verdict != "EXECUTABLE"`, and _upsert returns without writing when the current value matches `^\s*NOT_ACTIONABLE\s*\(\s*final_publication\s*\)`. scripts/harness_w1a6.py S12.14b, S12.15b and S12.18 pin this ratchet (re-run here: 94/94 PASS).
- finalize_decision_publication.py: publish_blocker only downgrades; feed_promoted is always False. main() binds run_id to GITHUB_RUN_ID, which is right only inside daily_sync's own final job. In a separate workflow GITHUB_RUN_ID is the release run, and every cohort check fails (harness N3 reproduces the exact four FAIL codes: PF_SOURCE_PUBLICATION_COHORT, PF_SOURCE_CONSUMPTION_UNPROVEN, SOURCE_PUBLICATION_COHORT, T10_SOURCE_CONSUMPTION_UNPROVEN). final.run(run_id='local', publish=False) silently runs the NON-strict audit (harness N6: exit 0, no expected_source_run in the policy).
- apps_script/16_Decision_Top10.gs dt10UvParse_: the verdict token is the text before the first '|', it must equal 'EXECUTABLE' exactly, and the age is measured from the FIRST space-separated 'YYYY-mm-dd HH:MM:SS' stamp anywhere in the value, inclusive at DT10_UV_MAX_AGE_MIN = 480 minutes, fail-closed when more than DT10_UV_FUTURE_SKEW_MIN = 15 minutes in the future.
- scripts/send_digest.py, scripts/run_daily_brief.py and scripts/tfb_acceptance.py (D10-4c) read only the head token and never the age.

## What changed

New files:

- scripts/release_decision_feed.py, SCRIPT_VERSION 1.0.0, dated WHY block citing blocker 1 / P0-1 / D1. It imports the unchanged finalizer and freshness audit and adds no acceptance thresholds of its own.
- .github/workflows/release_decision_feed.yml, VERSION 1.0.0. workflow_dispatch only, inputs source_run_id (required, no default) and mode (release, sweep, kill). permissions contents: read and actions: read. Its own concurrency group tfb-decision-release with cancel-in-progress false (NOT the production write lease). environment production, runs-on ubuntu-24.04, timeout 30 minutes. The job env is the verify-final-decision-publication env of daily_sync.yml copied verbatim (all TFB_DECISION_* and TFB_EXPECTED_MIN_ROWS_* lines and their comment), with TARGET_SHEET_ID fixed to vars.DEFAULT_SPREADSHEET_ID (no inputs fallback), plus the two gates mapped as `${{ vars.TFB_DECISION_RELEASE || '0' }}` and `${{ vars.TFB_DECISION_RELEASE_KILL || '0' }}`. Inputs reach the shell through step env only. The credentials step, artifact upload and secure cleanup are copied from the final job (the harness asserts the run blocks are identical).
- tests/test_release_decision_feed.py, the harness (walk, negatives, golden negatives, node parity).
- this sheet.

Edited files (additive only):

- scripts/verify_deployment.py: SCRIPT_VERSION 1.0.55 -> 1.0.56 with a dated v1.0.56 WHY block. SCRIPTS gains ("release_decision_feed", "1.0.0", ...). FLAGS gains TFB_DECISION_RELEASE and TFB_DECISION_RELEASE_KILL, both with default "0" and kill-switch field False, and both join _WORKFLOW_SCOPED. kill=False for the KILL flag is deliberate: its unset state is the safe default, so scoring it as a disarmed protection would be a permanent false DRIFT. No verifier logic changed; the AST def inventory is 14 -> 14, 0 removed, 0 added.
- .github/workflows/ci.yml: VERSION 1.1.13 -> 1.1.14 with a dated WHY line (stacked on the runner-pin change, which takes 1.1.13); tests/test_release_decision_feed.py added to the blocking lean pytest list right after tests/test_final_publication_workflow.py. Nothing removed. The push path filter already covers scripts/**, tests/** and .github/workflows/**, and the py_compile step lists only decision-layer core modules, so neither needed a change.

Review fixes (a second commit on the same branch, 2026-10-13; the script, workflow and harness stay at their unreleased v1.0.0 and the verifier stays at v1.0.56 with the same 1.0.0 pin, so no pin changes; nothing removed):

- scripts/release_decision_feed.py: a fault on the compare-and-set re-read inside publish_release (a transport error, or a layout fault on that read) now raises ReleaseConflict with reason compare_and_set_read_failed. run() refuses with exit 3 and ZERO writes. Before, it fell into the generic "the write may have landed" branch and wrote a release_unconfirmed downgrade carrying release=<id>, so one transient Sheets 5xx made the run unreleasable (release_window_consumed) for a write that was never attempted. The update and readback keep the release_unconfirmed downgrade. Second, with the gate off, when the strict re-verification of a live release of SOURCE fails, the downgrade the armed path would write is reported as would_downgrade (still zero writes), so a dry-run makes it visible that disarming has not retracted anything. The module docstring gained a dated REVIEW FIXES block.
- tests/test_release_decision_feed.py: N24 now asserts a lower bound on the verifier version (>= 1.0.56) instead of exactly 1.0.56, so a later routine pin bump cannot turn this blocking lane red. New N25 (pre-write read fault). N17 checks would_downgrade. G5 became discriminating and G6 was added (see Measured results).
- .github/workflows/release_decision_feed.yml: header comments (the kill and concurrency behaviour, and the three-step retract), and the job-summary table also prints would_downgrade. No trigger, permission, concurrency, env line or step was added or removed.

Not touched, byte-identical to dda1211 (sha256 prefixes, checked by harness G4): finalize_decision_publication.py a971b052a9e4, run_dashboard_sync.py 021e1ced5dee, audit_decision_surface_freshness.py d88f4627171d, audit_full_refresh_coverage.py 22bcf13b4133, apps_script/16_Decision_Top10.gs 2aec8a98a1f2, apps_script/11_Manual_Refresh_Coordinator.gs 09714c3a8dad, apps_script/24_Sync_Dispatch.gs 8a9808a77c55. docs/flags.md was NOT regenerated: it is already stale at HEAD (scripts/flag_registry.py at dda1211 reports more names and dynamic reads than the committed file), and regenerating it here would mix unrelated churn into this commit. The registry already reconciles both new flags (consumer in the script, manifest entry, mapping in release_decision_feed.yml); regenerate in a separate doc-only commit.

How a release run works (the script's run order):

0. SOURCE must fullmatch [1-9][0-9]* (no I/O otherwise). A promotion with the gate armed also requires GITHUB_ACTIONS=true and a numeric GITHUB_RUN_ID.
1. One read of '_Status'!L1:M60 and the unchanged finalize.publication_slot; any layout fault is exit 3.
2. Kill (KILL variable armed or mode=kill): never promote; if any consumer would read the prior as EXECUTABLE, write one release_killed downgrade.
3. Sweep: an EXECUTABLE prior that is expired, future-stamped or outside the release grammar is retracted (release_expired or release_invalid). mode=sweep always writes; mode=release writes only when armed and otherwise reports would_downgrade.
4. classify_prior (skeptic amendment 5): absent, an older-run NOT_ACTIONABLE, or a same-run NOT_ACTIONABLE(<any reason>) without release= are eligible. Refused: run-less owner holds, ambiguous or non-numeric run tokens, newer runs, EXECUTABLE of another run, a same-run value carrying release= (the window for that run is spent), anything carrying superseded_by_run, and any other head (lowercase, ACTIONABLE, 'stale', 'EXECUTABLE(release)'). A same-run release in grammar is already_released.
5. Provenance of SOURCE (GitHub REST, job token): path .github/workflows/daily_sync.yml, head_branch main, event schedule or workflow_dispatch, status completed, exactly one 'Verify final decision publication' job that completed with success or failure. API failure is deferred (exit 1); a mismatch is refused (exit 3). Then the in-flight probe: any schedule or workflow_dispatch run of daily_sync.yml, page_refresh_recovery.yml or grid_capacity_repair.yml whose status is not completed (queued, in_progress, waiting, pending, requested) defers with exit 1; push and pull_request runs are ignored; a probe error defers (fail closed).
6. result = await finalize.run(sheet_id, publish=False, max_rows=20000, run_id=SOURCE).
7. accepted() requires exit 0, audit codes [0, 0], no blockers, run_id == SOURCE, policy.expected_source_run == SOURCE, decision executable, both summary exit codes 0, no decision findings, and both cockpit run stamps present. The expiry anchor is the OLDEST audited decision input, min(My_Portfolio publication, Portfolio_Decision run, Top_10 run) from the strict decision payload. The release is refused when anchor + 480 minutes is already reached, and also when the operator has tightened the decision-age cap below 480 minutes (the cockpit's fixed clock would otherwise outlive the tightened cap). When not accepted and armed, a live release of SOURCE is retracted with the finalize blockers as the reason. When not accepted and the gate is off, nothing is written and that retraction is reported as would_downgrade.
8. already_released: no write and no expiry extension.
9. Gate off: dry-run report with the exact value that would be written.
10. Armed: re-probe in-flight runs immediately before the write, compare-and-set (slot and prior pair unchanged), ONE RAW values.update on L{slot}:M{slot}, exact readback. A conflict before the write (slot or prior changed: compare_and_set_failed) and any fault on the compare-and-set re-read itself (compare_and_set_read_failed) are exit 3 with zero writes, and the run's release window is kept, so a later dispatch can still release it. Any failure once the update was attempted (the update call or the readback) leaves a best-effort release_unconfirmed downgrade (exit 3).

The value grammar (the only EXECUTABLE value this script can write; publish_release refuses anything else):

```
EXECUTABLE | run=90001 | 2026-07-31 20:00:00+03:00 | issued=2026-07-31T21:00:00+03:00 | expires=2026-08-01T04:00:00+03:00 | ttl_min=480 | release=777001 | attempt=1 | receipts=T10:90001,PF:90001 | t10_at=2026-07-31T20:10:00+03:00 | pf_at=2026-07-31T20:05:00+03:00 | mp_at=2026-07-31T20:00:00+03:00 | audits=0,0
```

The only space-separated stamp is the anchor, so the cockpit's 480-minute clock starts at the oldest audited input and equals the audit's 8-hour decision-age cap. Every other datetime is T-form, which the cockpit regex cannot match. There is exactly one run= token. attempt= is the SOURCE run's run_attempt from the provenance read. Downgrades written by this script use the unchanged publish_blocker and have the form 'NOT_ACTIONABLE(final_publication) | run=<run> | <now +03:00> | <reason> | release=<this run id>', which the sync preserves.

## Default OFF identity

With TFB_DECISION_RELEASE unset (the workflow maps it to '0'), mode=release performs reads only: one L1:M60 read, the provenance and in-flight reads, and the strict audits. It writes nothing, even when the prior is an expired release, when the audits fail, or when a malformed EXECUTABLE is present (harness N17 checks five such priors for zero updates and identical rows; W1 checks that the only Sheets call is one get). It never retracts either: when a live release of SOURCE fails its strict re-verification with the gate off, the retraction is only reported as would_downgrade (N17). Disarming the gate therefore never withdraws a live release; only mode=kill, mode=sweep or an armed failed re-verification write a downgrade. The kill and sweep downgrades are allowed with the gate off because they only reduce permission (skeptic amendment 6); they write only through the unchanged finalize.publish_blocker. No existing module changed behaviour: the finalizer, sync, audits and Apps Script are byte-identical (G4), the verifier only gained registry rows, and ci.yml only gained one test path. The workflow is dispatch-only, so nothing runs unless the owner dispatches it.

## Measured results

All commands were run in the worktree with /home/user/tfb-venv/bin/python. Base copies were made with `git show dda1211:<path>` into the scratch directory and passed as TFB_TEST_FINALIZE_BASE and TFB_TEST_SYNC_BASE (G4, G5 and G6 also read sibling copies of the audits, the three .gs files, verify_deployment.py and release_decision_feed.py from the same directory). The base release_decision_feed.py copy is EMPTY on purpose: `git show dda1211:scripts/release_decision_feed.py > <base>/release_decision_feed.py` fails because the path does not exist at dda1211 ("exists on disk, but not in 'dda1211'"), and the shell redirect leaves an empty file, which is exactly the base state (no release step). The numbers below are from the review-fix commit.

Harness under pytest, without and with the base copies:

```
python -m pytest -q tests/test_release_decision_feed.py
31 passed, 6 skipped in 46.58s
TFB_TEST_FINALIZE_BASE=<base>/finalize_decision_publication.py TFB_TEST_SYNC_BASE=<base>/run_dashboard_sync.py python -m pytest -q tests/test_release_decision_feed.py
37 passed in 53.16s
```

Harness as a script, twice with the base copies and once without:

```
python tests/test_release_decision_feed.py      (x2, base env set)
G4 byte-identical: 7 protected files
PASS 37/37
G4 byte-identical: 7 protected files
PASS 37/37
python tests/test_release_decision_feed.py      (base env unset, exit 0)
SKIP case_g1_base_finalize_cannot_release :: TFB_TEST_FINALIZE_BASE unset
... (G2-G6 likewise)
PASS 31/31  (skipped 6)
```

What the cases cover. The walk: W1 gate-off dry-run (one get, rows identical, expected_source_run 90001, would_release equals the exact value above); W2 armed release (calls exactly get L1:M60, get L1:M60, update '_Status'!L4:M4 RAW, get L4:M4; one space stamp 2026-07-31 20:00:00+03:00; expires=2026-08-01T04:00:00+03:00; send_digest reads EXECUTABLE; the brief sees no veto; the sync veto regex does not match); W3 re-dispatch at +10 minutes is already_released with zero updates and no expiry extension; W3b the REAL _write_upstream_verdict downgrades the release to NOT_ACTIONABLE(missing:ML) when page keys are missing and leaves it untouched with four fresh OK keys (UPSTREAM_READY diagnostic); W4 sweep at anchor + 480 minutes writes nothing and at + 480 minutes + 1 second writes exactly 'NOT_ACTIONABLE(final_publication) | run=90001 | 2026-08-01 04:00:01+03:00 | release_expired | release=777001', which the real sync then preserves, all with the release gate off; W5 a re-release of the same run is refused as release_window_consumed with no audit and no write. The negatives: N1 eleven invalid SOURCE values give exit 3 with zero I/O; N2 a 90002 cohort gives exit 2; N3 the release run id as SOURCE gives exit 2 with exactly the four cohort and receipt FAIL codes; N4 warning-only audits give exit 1; N5 failing audits give exit 2 and retract only a live release of the same run; N6 the 'local' result is rejected as not_strict; N7 the full prior-classification matrix, end-to-end refusals with no audit and no write, the malformed-EXECUTABLE retraction (armed) or report (dry-run), an older-run veto released and an absent key released into the first blank slot; N8 compare-and-set conflict, zero writes; N9 readback mismatch plus the release_unconfirmed downgrade attempt; N10 the 12 finalize layout grids; N11 probe busy, count, exception, wrong type, busy or failing re-probe before the write, provenance unavailable and five provenance mismatches; N12 kill with the gate off, kill over a veto, KILL variable with an armed gate and clean audits; N13 no promotion without GITHUB_ACTIONS=true and a numeric GITHUB_RUN_ID; N14 weakening env vars still yield exactly final.final_policy, and a tightened 4-hour cap refuses; N15 the TTL and skew equal the .gs constants and 8 h x 60; N16 workflow contract and full env parity with the final job; N17 gate off never writes in mode=release, sweep still allowed; N18 inputs 7h59m old give expiry exactly 1 minute later, and at exactly 480 minutes the audits pass but the release is refused as anchor_expired; N19 the probe ignores push and pull_request runs and counts waiting, pending and requested; N20 provenance rules; N21 parse_feed equals the real dt10UvParse_ under node on 19 values; N22 an AST check that finalize.run is called once with run_id=source, publish=False, max_rows=20000 and no service; N23 CLI exit 3 (not argparse 2); N24 verifier registration and gate vocabulary, with a lower bound (>= 1.0.56) on the verifier's own version rather than an exact value; N25 a transport fault and a layout fault on the compare-and-set re-read each give exit 3, reason compare_and_set_read_failed, zero updates and an unchanged cell, and the same run then releases once the fault clears (the window is kept), while a fault on the update itself still gives release_unconfirmed. N17 also checks that a failed re-verification of a live release with the gate off is reported as would_downgrade ('... | coverage_failed | release=777001') with zero writes.

Node on the real dt10UvParse_ (the .gs loaded in a vm), the W2 value at 0, 480 and 481 minutes after the anchor, and a copy whose expires= is moved to 2030 at 481 minutes (the T-form is never read as the stamp):

```
2026-07-31T20:00:00+03:00 {'state': 'EXECUTABLE', 'reason': '', 'ageMin': 0}
2026-08-01T04:00:00+03:00 {'state': 'EXECUTABLE', 'reason': '', 'ageMin': 480}
2026-08-01T04:01:00+03:00 {'state': 'NOT_ACTIONABLE', 'reason': 'verdict stale (481m old)', 'ageMin': 481}
2026-08-01T04:01:00+03:00 {'state': 'NOT_ACTIONABLE', 'reason': 'verdict stale (481m old)', 'ageMin': 481}
```

Golden negatives against dda1211. Read these honestly: G1-G4 are base-PROPERTY checks. The finalizer, the sync, both audits and the three .gs files are byte-identical between dda1211 and HEAD (that is the point of this design: the new behaviour lives only in a new file), so G1-G4 hold on a HEAD copy too and cannot tell base from HEAD. G1 the base finalizer with publish=True on the clean fixture makes zero Sheets calls, leaves the W0 veto byte-identical and reports feed_promoted False; G2 the base sync with four fresh OK page keys writes UPSTREAM_READY to the diagnostic and leaves the veto unchanged; G3 the release run with the base finalizer swapped in gives a byte-identical result JSON and the same accepted verdict, which proves no policy is duplicated; G4 seven protected files are byte-identical. The DISCRIMINATING golden negatives are G5 and G6 (added by the review fix). G5: the verify_deployment.py def inventory is 14 -> 14, and the N24 registration (the release_decision_feed 1.0.0 pin, both flags in FLAGS and in _WORKFLOW_SCOPED, read by AST literal) holds on this tree and is ABSENT from the base verifier. G6: the W2 armed walk (TFB_DECISION_RELEASE=1, GITHUB_ACTIONS=true) is first run on this tree as a positive control (the cell becomes the W2 value), then against the base tree: the base release_decision_feed.py is empty, so the armed walk reduces to the base final job (publish=True) plus the base sync legs, and the W2 expectation (an EXECUTABLE cell) FAILS: the cell stays the W0 veto and no update touches the feed key. If the base copy were a real module, G6 would load it and run its armed release instead.

Discrimination proof. With every base-dir file replaced by a HEAD copy (finalize, sync, audits, .gs, verify_deployment.py and release_decision_feed.py), the same command fails, exit 1:

```
TFB_TEST_FINALIZE_BASE=<headcopy>/finalize_decision_publication.py TFB_TEST_SYNC_BASE=<headcopy>/run_dashboard_sync.py python tests/test_release_decision_feed.py
ok   case_g1_base_finalize_cannot_release
ok   case_g2_base_sync_never_lifts_the_veto
ok   case_g3_release_with_base_finalize_is_identical
ok   case_g4_protected_sources_byte_identical
FAIL case_g5_verifier_ast_inventory_zero_removed
AssertionError: base verifier already pins release_decision_feed: TFB_TEST_FINALIZE_BASE is not a base copy
FAIL case_g6_armed_walk_fails_on_base_tree
AssertionError: the base tree released the feed: the base dir holds a HEAD copy, not dda1211
PASS 35/37
```

With a HEAD copy that omits release_decision_feed.py (the reviewer's headcopy layout), G5 still fails and G6 skips (`pytest -k "g5 or g6"`: 1 failed, 1 skipped). A pointer at a wrong directory is therefore noticed whenever the sibling verify_deployment.py is supplied.

Review-fix probes (scratch runner, in-process, the module swapped for the previous commit 4233368's copy): N25 FAILS on the previous module ('transport', ['release_unconfirmed']) and passes on the fixed one; N17 FAILS on the previous module (no would_downgrade) and passes on the fixed one. The reviewer's scratch test_transient_cas_read_burns_release_window now prints, for the first dispatch, "refused ['compare_and_set_read_failed'] ... before any write", the cell unchanged and zero updates, and for the second dispatch "released [] eligible" (before the fix: release_unconfirmed, then release_window_consumed). N24 with the verifier version patched in-process: 1.0.57 pass, 1.1.0 pass, 1.0.56 pass, 1.0.55 fail.

Mutation probes (scratch runner, in-process; re-run on the review-fix commit with the same result): dropping the anchor-spent refusal turns N18 red; dropping compare-and-set turns N8 red; allowing a same-run re-release turns W5 red; accepting non-strict results turns N6 red; TTL 481 turns N15 red; forcing the gate on turns N17 red. Each case is green again on the clean module. Result: MUTATIONS KILLED 6/6.

Related existing suites:

```
python -m pytest -q tests/test_final_decision_publication.py tests/test_decision_surface_freshness.py tests/test_final_publication_workflow.py tests/test_verify_manifest_pins.py tests/test_verify_optional_rollout_flags.py tests/test_flag_registry.py tests/test_repository_workflow_audit.py tests/test_workflow_flag_mappings.py tests/test_ci_required_verdict.py tests/test_track_flags_mapped.py tests/test_release_decision_feed.py tests/test_no_import_side_effects.py
334 passed, 6 skipped, 39 subtests passed in 154.70s
python -m pytest -q <the full ci.yml blocking lean list, 91 files, read out of ci.yml; base env unset, so G1-G6 skip>
3469 passed, 9 skipped, 2 warnings, 131 subtests passed in 278.45s
python scripts/harness_w1a6.py --deterministic
HARNESS v2.4.0 RESULT: 94/94 PASS  (1 suite(s) skipped)
python tests/test_verify_manifest_pins.py
[MANIFEST PIN GUARD] 3/3 PASS
python scripts/audit_repository_workflows.py
workflow audit: errors=0 warnings=2      (both pre-existing daily_sync TEMPORARY_OVERRIDE_ACTIVE warnings)
python scripts/verify_deployment.py
TFB DEPLOYMENT VERIFIER v1.0.56 ... [GH-ENV] TFB_DECISION_RELEASE 0 ... [GH-ENV] TFB_DECISION_RELEASE_KILL 0 ... modules 63 (0 drift, 0 missing)
```

The local verifier verdict is DRIFT because two Render kill-switches (TFB_ADV_ENGINE_CALL_TIMEOUT, TFB_OPP_BUILD_OFFLOOP) default to disarmed in this shell, which this change does not touch; neither new flag contributes, because workflow-scoped flags are excluded from the verdict.

## Known limits

- P0-1 alone cannot produce a live EXECUTABLE board. Blockers 2 (ordering), 3 (name coverage), 4 (My_Portfolio) and 5 (the Portfolio_Decision receipt) each fail the same strict audits. Expect exit 2 dry-runs until they are fixed.
- Residual race. The private concurrency group, the fail-closed probe (re-run immediately before the write) and compare-and-set leave a window of seconds in which a sync can start after the last probe. Joining the production lease instead would cancel pending syncs. The window is closed for real only by arming prerequisites (2) and (4) below.
- A release of run A stays EXECUTABLE until A's 480-minute clock ends or a later writer downgrades it. A later sync's legs downgrade it only when TFB_SYNC_UPSTREAM_VERDICT is armed and their composite is not EXECUTABLE; the later sync's final job re-vetoes it. Until prerequisite (2) lands, the cockpit does not compare the feed's run= with its own cohort.
- The age-blind consumers (run_daily_brief, send_digest, tfb_acceptance D10-4c) keep treating an expired EXECUTABLE as live until a sweep runs. The sweep is dispatch-only; no schedule is added here.
- Checkout is this workflow's own ref (github.sha), refused unless dispatched from refs/heads/main. The release therefore runs main's reviewed audit code, which may be newer than the commit the source sync ran. This is a deliberate deviation from the design's head_sha checkout: it lets a fix to the release apply to an existing cohort, and the audit modules are byte-identical across this change anyway.
- A rerun of the SOURCE run re-uses its run id. While it runs, the probe defers new releases; after it completes, provenance records the new attempt in attempt=. The final job's veto carries no attempt, so a release cannot prove the attempt that produced the veto; it re-proves the whole cohort strictly instead.
- LIVE-WINDOW RISK on a rerun ("Re-run all jobs") of a released SOURCE X. The sync stamps GITHUB_RUN_ID, which is unchanged across attempts (run_dashboard_sync.py has no GITHUB_RUN_ATTEMPT read), so attempt 2 re-stamps every page with the same run=X. A live release for X (attempt=1) stays EXECUTABLE over attempt-2 data that no audit has checked until attempt 2's final job re-vetoes it (or its legs downgrade it, only when TFB_SYNC_UPSTREAM_VERDICT is armed and the composite is not EXECUTABLE). A cockpit render inside that window shows EXECUTABLE. Prerequisite (2) does not close this, because the feed's run= and the cockpit cohort are both X; nothing checks attempt= today. Prerequisite (4) below is widened to cover it. Until then, do not rerun a released SOURCE while the gate is armed; dispatch mode=kill first.
- mode=kill shares the private group tfb-decision-release (cancel-in-progress false) with releases. A kill dispatched while a release runs waits for that release, audits included (up to the 30-minute job timeout), so it is not immediate. GitHub also cancels a PENDING run in a group when a newer run queues into it, so a release dispatched after a queued kill silently cancels the kill (documented platform behaviour, not observed live). Moving kill to its own group would not help on its own: a concurrent kill could read the veto, write nothing, and then the in-flight release would write EXECUTABLE. What makes a kill robust is the sticky variable: every run that starts after TFB_DECISION_RELEASE_KILL=1 is set, whatever its mode, takes the kill branch and retracts any EXECUTABLE, so the cancelling dispatch itself retracts. Hence the three-step kill under Rollback (variable, dispatch, readback).
- The in-flight probe lists the 50 most recent schedule and workflow_dispatch runs per workflow. A production run older than that and still not completed would be missed; at three schedule slots a day this needs more than two weeks of queueing.
- The verifier prints its generic workflow-scoped note "(set in daily_sync.yml)" for the two new flags; they actually live in release_decision_feed.yml. Changing that text would be verifier logic, so it is left as is.
- A tightened TFB_DECISION_SURFACE_MAX_AGE_H below 8 refuses every release, because the cockpit's 480-minute clock cannot be shortened from the value without moving the anchor.
- Out-of-repo Apps Script files that might write the key could not be checked from here.

## Operator steps

ARMING PREREQUISITES, all owed and NOT built in this commit. Do not arm TFB_DECISION_RELEASE before each one is done:

1. Portfolio_Decision producer receipt (blocker 5). The out-of-repo 10_My_Portfolio.gs must write exactly one 'source readiness VERIFIED (run <id>)' in Portfolio_Decision!B2, derived from My_Portfolio cohort consumption and NOT from dt10BoardVerdict_ (which folds in the feed), or the release can never bootstrap. Without it every release exits 2 on PF_SOURCE_CONSUMPTION_UNPROVEN.
2. A GAS cohort-binding check in dt10BoardVerdict_: when the parsed feed state is EXECUTABLE and the feed's single run= differs from the cockpit cohort, return NOT_ACTIONABLE('feed run X != cohort Y'). Gate it by a Script Property (default off, byte-identical when off) with a node test on the real .gs. This closes the window in which a release for run A shows EXECUTABLE over a newer, unaudited cohort B.
3. A cockpit re-render after the release. The order is sync -> cockpit refresh (writes the receipts) -> this release -> cockpit refresh (shows the board). Until the cockpits re-render, Top_10 B2 still says 'output: WITHHELD' and the brief keeps its veto; the release JSON says display_pending_cockpit_refresh: true. The P0-1 success check must read a Top_10 B2 rendered after the release.
4. Supersede on every sync attempt start: a step at the start of EVERY production sync attempt, reruns included (under its lease, before any leg), that downgrades any EXECUTABLE feed, adding superseded_by_run=<run> and the attempt, whenever the value's run= is not this run OR its attempt= is not this GITHUB_RUN_ATTEMPT (a same-run rerun re-stamps every page with the same run=; see Known limits). The simplest correct form downgrades any EXECUTABLE at the start of every attempt. This script already refuses any value that carries superseded_by_run.
5. The age-blind consumers run_daily_brief, send_digest and tfb_acceptance (D10-4c) need an expiry check (or a scheduled sweep) so an expired release is not honoured between 480 minutes and the next veto.

Zero-code observe experiment (safe today, the gate stays off): after a scheduled sync completes and BOTH cockpits (Top_10 and Portfolio_Decision) have been refreshed, dispatch 'Release Decision Feed (P0-1)' from main with mode=release and source_run_id = that sync's run id. Download the artifact release-decision-feed-<run id> and read release_decision_feed.json: outcome, reasons, audit_exit_codes, the decision findings and would_release. Expect exit 2 with PF_SOURCE_CONSUMPTION_UNPROVEN until prerequisite 1 lands; the T10 receipt check should pass if the Top_10 refresh followed the sync.

Commit: this change is two commits on a review branch (the build, then the review fixes) over the six files listed under What changed; the owner reviews them and pushes. Nothing is pushed from the build session.

Arming, only after prerequisites 1-5 and a dispatched dry-run artifact showing accepted: true with both receipts:

1. Confirm the production environment's protection rules allow this workflow and supply GOOGLE_SHEETS_CREDENTIALS or GOOGLE_SHEETS_CREDENTIALS_B64.
2. Set the repository variable TFB_DECISION_RELEASE=1. Keep TFB_DECISION_RELEASE_KILL unset or 0.
3. After a sync and a cockpit refresh, dispatch mode=release with the sync's run id. Read back: job summary banner ARMED, outcome released, the _Status feed cell equal to the value in the JSON, then refresh the cockpits and read Top_10 B2 'output: EXECUTABLE' for the same cohort.
4. Dispatch mode=sweep with any run id after expiry to retract an expired release for the age-blind consumers.

Rollback:

- Retract a live release, all three steps: (1) set the repository variable TFB_DECISION_RELEASE_KILL=1. This blocks every later promotion until unset, but it is read only when a run starts and writes nothing by itself: a live EXECUTABLE stays on the cockpit for up to 480 minutes, and with no time limit for the age-blind brief, digest and acceptance consumers, until a run retracts it. It also does not stop a release run that is already in flight. (2) Dispatch mode=kill (works with the release gate off). If a release is running, the kill waits for it and then retracts what it wrote. (3) Read back the _Status 'TFB Decision Feed' cell and confirm it is NOT_ACTIONABLE(final_publication) ... release_killed. If the kill run shows as cancelled (a later dispatch replaced it in the queue) or the cell still reads EXECUTABLE, dispatch mode=kill again; with the variable set, any later run also retracts.
- Disarm: set TFB_DECISION_RELEASE to 0 or delete it; mode=release returns to the read-only dry-run. Disarming NEVER retracts a live release: with the gate off a failed re-verification only reports would_downgrade. Use the three-step retract above to withdraw the board.
- Remove: delete scripts/release_decision_feed.py, the workflow and the harness, revert the two ci.yml lines and the verify_deployment.py v1.0.56 entries. Nothing else depends on them.
- DT10_UPSTREAM_VERDICT=off is a cockpit BYPASS that forces EXECUTABLE. Never use it as a kill switch.

## Owner decisions owed

- D1 GO for this build and for the order sync -> refresh -> release -> refresh.
- Whether to add a sweep-only schedule (it would be the 20th scheduled workflow) or a separately gated expiry check in the brief, digest and acceptance consumers (prerequisite 5).
- Who builds prerequisites 1, 2 and 4 (two are Apps Script pastes, one is a daily_sync step) and in which order.
- Whether GAS-dispatched syncs should be releasable: 24_Sync_Dispatch.gs sends only run_mode full_sync, so My_Portfolio keeps an older run= and those cohorts always fail PF_SOURCE_PUBLICATION_COHORT. Either restrict releases to schedule cohorts or add a gated include_portfolio to that dispatch (a separate commit).
- P0-2: a fine-grained token with Actions read and write if the GAS should dispatch this workflow itself; until then dispatch manually after the cockpit refresh. If P0-2 is built, its dispatcher must not dispatch mode=release while TFB_DECISION_RELEASE_KILL is set or while a kill run of this workflow is queued (a newer dispatch cancels a pending kill), and the owner decides whether this workflow should gain a run-name carrying the mode so that check can see it.
- Whether to build the widened prerequisite (4) as "downgrade any EXECUTABLE at the start of every sync attempt" (simplest, closes the rerun window) or as an attempt= comparison.
- Live reads this session cannot make: the current _Status L1:M60 values, the repository variables TFB_SYNC_UPSTREAM_VERDICT, TFB_SYNC_VERDICT_MAX_AGE_MIN and DEFAULT_SYNC_KEYS, and whether the 13_AutoRefresh cockpit triggers are installed.
- A separate doc-only commit to regenerate docs/flags.md.
