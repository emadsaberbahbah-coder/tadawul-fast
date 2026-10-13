# TFB Commit Sheet - runner image pin (ubuntu-latest -> ubuntu-24.04) and manual image canary

Date: 2026-10-13 (Tuesday). Base: origin/main at dda1211. Lane: GitHub Actions configuration only. ci.yml moves from v1.1.12 to v1.1.13.

## Why this item

GitHub is moving the `ubuntu-latest` runner label to a newer Ubuntu image (26.04) starting Monday 19 Oct 2026. At dda1211 every job in this repository ran on `ubuntu-latest`: 55 labels in 34 workflow files, including every scheduled writer (daily_sync, track_performance, shadow_board, shadow_scorer, pit_snapshot, workbook_backup, digest and others). When the label moves, all of those jobs would start running on a different operating system release on the same day, with no commit, no review and no test run in between.

The jobs do depend on what the image ships. The daily_sync preflight job checks for the image's own `curl`, `jq`, `python3` and `pip3` and does not install them; if one is missing, every scheduled Daily Sync fails at preflight. Several jobs use `jq` directly. The lean CI lane runs six Node harnesses and some Python tests start `node`, which comes from the image. Every Python step uses `actions/setup-python@v6`, which needs a Python build for the image's Ubuntu release (3.11 almost everywhere, 3.12 in grid_capacity_repair, 3.13 in the advisory framework-drift lane). Pinning `ubuntu-24.04` stops the silent swap. Today `ubuntu-latest` resolves to `ubuntu-24.04`, so the pin changes nothing now. It only prevents the change on 19 Oct.

## Evidence

At dda1211, `grep -c "runs-on: ubuntu-latest"` over `.github/workflows/*.yml` gives 55 labels in 34 files. That is every `runs-on` in the repository, and every job has one (no reusable-workflow jobs). No workflow uses apt-get, sudo, docker, `container:` or `services:`, so nothing installs the tools it relies on. tests/ and scripts/ never referenced `runs-on` or `ubuntu` before this item, so no existing test depended on the label. The daily_sync preflight tool check is the step with id `validate` in job `preflight-validation`. The read-only review behind this item is in the scratchpad file wfA/5_R-repo.md, section (2) and SKEPTIC amendment (2). It confirmed the 55/34 count and asked for three things, all done here: a separate commit and version for the pin, a canary in its own workflow with its own concurrency group, and a canary that mirrors the daily_sync preflight check, `node --version` and setup-python 3.11. It also asked that no new TFB_ flag be added.

## What changed

1. Labels. In all 34 existing workflow files, every `    runs-on: ubuntu-latest` line became `    runs-on: ubuntu-24.04` (55 lines). `git diff` shows exactly 56 removed lines: the 55 label lines and ci.yml's old VERSION line. Nothing else was removed.

2. Version headers.
   - ci.yml: header bumped to `VERSION 1.1.13 (2026-10-13)`, with a v1.1.13 entry and a dated WHY block covering why the pin, the un-pin procedure and rollback. The harness was also added to the lean pytest list (one line, after `tests/test_verify_optional_rollout_flags.py`).
   - Workflows with a `# VERSION x.y.z` header got a new VERSION line plus a one-line dated WHY, added above the old VERSION line, which is kept as history (the same layout ci.yml already uses for its older VERSION lines). These are daily_sync 1.0.1 -> 1.0.2, track_performance 1.0.1 -> 1.0.2, shadow_scorer 1.0.0 -> 1.0.1 and dependency_typing_review 1.0.0 -> 1.0.1.
   - Workflows whose header holds a version-history list got a new dated entry at the end of that list: board_scorecard v1.0.0 -> v1.0.1, pit_snapshot v1.1.0 -> v1.1.1, shadow_board v1.2.0 -> v1.2.1, workbook_backup v1.2.0 -> v1.2.1 and provider_target_coverage W1B-1 v1.1 -> v1.1.1.
   - The other 24 files have no version in their header block (the comments before `on:`), so they got the label change only: acceptance_check, actions_health_review, backtest_harness, backtest_signals, calendar_sync, core_repair_readback, daily_brief, decision_surface_freshness, digest, full_refresh_coverage, grid_capacity_repair, intraday_refresh, investment_policy_ci, investment_policy_shadow, manual_refresh_priority, news_archive, page_refresh_recovery, repair_stores, repository_workflow_audit, sync_outcome_audit, tests, thesis_exit_backtest, weekly_brief and workflow_repair_tests. digest.yml names a v1.1.0 inside its CONCURRENCY section comment but not in its header, so it is in this group. The version tokens in acceptance_check, repair_stores, grid_capacity_repair and similar files name the scripts they run, not the workflow.

3. New `.github/workflows/runner_image_canary.yml` (VERSION 1.0.0). It runs only on `workflow_dispatch`, with one input, `runner_image`, a choice of ubuntu-26.04 (the default) or ubuntu-24.04 (a control). Its concurrency group is `runner-image-canary-${{ inputs.runner_image }}-${{ github.ref }}` with cancel-in-progress false. That group is not `ci-*` and not the production write lease, and no other workflow uses it, so the canary can neither cancel nor be cancelled by CI or a sync. Its permissions are `contents: read`, its timeout is 20 minutes, it uses no secrets and it writes nothing. Its steps are:
   - checkout with persist-credentials false;
   - the daily_sync preflight tool check, copied character for character (the harness compares the two run strings);
   - a check that `/etc/os-release` reports the requested release (`ubuntu-${VERSION_ID}` must equal the input), so a canary cannot pass green on the wrong image;
   - `node --version`, plus jq, curl, python3 and pip3 versions;
   - setup-python@v6 with 3.11;
   - `pip install -r requirements.txt pytest`;
   - `compileall` of main.py, core, routes and scripts;
   - six fast offline tests that the blocking lean lane also runs;
   - the Apps Script syntax harness on the image's node.

   The input reaches run blocks only through `env:`. There is no `${{ }}` expression inside any run block.

4. New `tests/test_runner_pin_ubuntu2404.py` (HARNESS_VERSION 1.0.0). It runs as a script (prints `PASS k/k`, exits 0 or 1) and under pytest (one parametrized test per case). Its cases are:
   - P1: no `ubuntu-latest` anywhere in the parsed YAML of any workflow, and none on any line that is not a full-line YAML comment.
   - P2: every `runs-on` is exactly `ubuntu-24.04`, except the canary job, whose value must be exactly `${{ inputs.runner_image || 'ubuntu-24.04' }}`.
   - P3: the canary contract described in item 3.
   - P4: scripts/audit_repository_workflows.py reports errors=0 on the tree and nothing at all for the canary file.
   - P5: the label-only equality proof (needs the base copies).
   - P6 and P7: the golden negatives (need the base copies).
   - P5 to P7 skip cleanly when `TFB_RUNNER_PIN_BASE_DIR` is unset.

   A second variable, `TFB_RUNNER_PIN_WORKFLOWS_DIR`, points the gate cases at another directory, so the golden negative can also be run by hand.

5. This sheet.

No Python module was changed. An AST def inventory of all 377 .py files at dda1211 against the working tree reports 0 changed files, 0 removed defs and 0 added defs. The one new .py file is the harness, with 28 defs. scripts/verify_deployment.py pins no workflow version, so no manifest pin and no SCRIPT_VERSION bump were needed.

## Default OFF identity

The pin cannot sensibly be ENV-gated. A `${{ vars.X || 'ubuntu-latest' }}` label would leave every job on the moving label by default, which is exactly the risk this item removes. Following the review's amendment, no TFB_ flag was added (the registry count is unchanged). Identity holds in a different sense: `ubuntu-24.04` is the image `ubuntu-latest` resolves to today, so every job runs on the same image as before. The harness proves (P5) that, apart from the labels and the one declared lean-list enrolment line in ci.yml, every existing workflow parses to the identical YAML document it had at dda1211, so no job, step, trigger, cron, concurrency group, timeout, permission or env value moved. The canary is off by default in the strict sense: it has no schedule, push or pull_request trigger and does nothing unless the owner dispatches it.

## Measured results with exact commands and outputs

Base copies were created with `for f in $(git ls-tree --name-only dda1211 .github/workflows/); do git show dda1211:$f > $B/$(basename $f); done`, where `B=<scratchpad>/build/runner-pin-ubuntu2404/base_workflows`. Python is /home/user/tfb-venv/bin/python.

`python tests/test_runner_pin_ubuntu2404.py` (base copies not set) printed:

    PASS P1 no ubuntu-latest in workflow content | 0 ubuntu-latest in 35 workflow files
    PASS P2 every runs-on pinned (canary expression excepted) | 55 jobs on ubuntu-24.04 + 1 canary expression
    PASS P3 canary workflow is manual, isolated and read-only | runner_image_canary.yml: dispatch-only, own group, contents: read, no expression in run
    PASS P4 repository workflow audit errors=0 | workflow audit errors=0 warnings=2; canary findings=0
    SKIP P5 base: label-only yaml.safe_load equality | TFB_RUNNER_PIN_BASE_DIR unset
    SKIP P6 base golden negative: 55 unpinned labels | TFB_RUNNER_PIN_BASE_DIR unset
    SKIP P7 base golden negative: gate cases fail on base | TFB_RUNNER_PIN_BASE_DIR unset
    PASS 4/4 (3 skipped: TFB_RUNNER_PIN_BASE_DIR unset)
    exit 0

`TFB_RUNNER_PIN_BASE_DIR=$B python tests/test_runner_pin_ubuntu2404.py` printed:

    PASS P5 base: label-only yaml.safe_load equality | 34 base files equal after 55 label substitutions; only new file runner_image_canary.yml
    PASS P6 base golden negative: 55 unpinned labels | pin check on base: 55 unpinned labels in 34 files, values ['ubuntu-latest']
    PASS P7 base golden negative: gate cases fail on base | gate cases red on base: P1=FAIL, P2=FAIL, P3=FAIL
    PASS 7/7
    exit 0

(P1 to P4 printed the same lines as above.)

The golden negative run by hand, `TFB_RUNNER_PIN_WORKFLOWS_DIR=$B TFB_RUNNER_PIN_BASE_DIR=$B python tests/test_runner_pin_ubuntu2404.py`, printed `FAIL P1 ... 89 hit(s)` (the 55 label lines plus 34 parsed-content hits), `FAIL P2 ... 55 unpinned`, `FAIL P3 ... runner_image_canary.yml is missing`, `PASS P4` (the audit has no runner rule, which is expected) and `FAIL P5 ...`. The last line was `FAIL 4/7 (PASS 3/7)`, exit 1.

Under pytest:
- `python -m pytest -q tests/test_runner_pin_ubuntu2404.py` gave `4 passed, 3 skipped`.
- With `TFB_RUNNER_PIN_BASE_DIR=$B` it gave `7 passed`.
- With both variables pointing at the base it gave `4 failed, 3 passed`.

Mutation checks against scratch copies of the current workflows each turned the right case red:
- Changing one timeout in repository_workflow_audit.yml: P5 FAIL ("parsed YAML differs beyond the label substitution").
- Deleting the lean enrolment line from ci.yml: P5 FAIL ("declared extra line(s) ... found []"), and P3 FAIL because the canary then names a test that is not in the lean lane.
- Giving the canary the group `ci-${{ github.ref }}` and putting `${{ inputs.runner_image }}` inside a run block: P3 FAIL on four counts.
- Dropping pip3 from the canary's tool loop: P3 FAIL ("not copied verbatim").
- Adding a workflow with `matrix.os: [ubuntu-latest]`: P1 and P2 FAIL.

Workflow audit: `python scripts/audit_repository_workflows.py` printed `workflow audit: errors=0 warnings=2`. On a scratch root holding the dda1211 copies it printed the same. Both warnings are the existing TEMPORARY_OVERRIDE_ACTIVE lines in daily_sync.yml, shifted by two lines. `python -m unittest tests/test_repository_workflow_audit.py` ran 7 tests, OK.

Related existing tests:

    python -m pytest -q tests/test_ci_required_verdict.py tests/test_workflow_flag_mappings.py tests/test_workflow_activation_observe.py tests/test_track_flags_mapped.py tests/test_activation_rollout_evidence_workflows.py tests/test_approved_universe_workflow.py tests/test_final_publication_workflow.py tests/test_name_identity_workflow.py tests/test_repository_workflow_audit.py tests/test_tracker_record_only_workflows.py tests/test_workflow_audit_repairs.py tests/test_flag_registry.py tests/test_scheduled_portfolio_refresh.py tests/test_sync_outcome_audit.py tests/test_sync_audit_cancelled_source.py tests/test_sync_rank_overall_page.py tests/test_core_repair_tiny_quotes.py tests/test_verify_manifest_pins.py tests/test_runner_pin_ubuntu2404.py

This gave `505 passed, 3 skipped, 115 subtests passed`. `python -m pytest -q tests/test_workbook_dq_audit.py` gave `12 passed, 1 skipped`.

The lean decision-layer step of ci.yml, extracted from the YAML and run verbatim with bash, gave `3442 passed, 6 skipped, 2 warnings, 131 subtests passed in 246.35s`.

The canary's run blocks were also executed as written on this Ubuntu 24.04.5 sandbox, with the input simulated as ubuntu-24.04. The pip install step was skipped because it needs the network. Results: the preflight check exited 0 and wrote `validated=true`, the release check exited 0, the tool versions step exited 0, compileall exited 0, the six tests gave `147 passed, 3 skipped`, and the Apps Script syntax harness reported 3 sources compiled. The release check with `REQUESTED_IMAGE=ubuntu-26.04` on the same 24.04 box exited 1 with `::error::runner reports ubuntu-24.04, requested ubuntu-26.04`.

Flag registry: `python scripts/flag_registry.py --output <scratch>` was run on this tree and with `--root` on a `git archive dda1211` copy. Both report "Discovered 870 flag names; 168 env reads use dynamic names". The two outputs are identical once workflow line numbers, which shift by the added header lines, are masked. docs/flags.md was not touched.

Other checks: `ruff check --select E9,F63,F7,F82 .` passed. `ruff check tests/test_runner_pin_ubuntu2404.py` with the repository's default rules passed. All added lines are ASCII.

## Known limits

- The pin fixes the Ubuntu release, not the tools. GitHub keeps updating the 24.04 image weekly, so node, jq, curl and the system Python can still change under a pinned label, as they could before.
- `ubuntu-24.04` will itself be retired by GitHub one day. The harness then fails loudly on a stale pin only once someone changes the label; nothing warns ahead of time, so a calendar reminder is the owner's tool here.
- Nothing here can confirm offline that GitHub moves `ubuntu-latest` on 19 Oct, that the target is 26.04, or that an `ubuntu-26.04` label exists yet. If the label does not exist, a canary dispatched on it just sits queued, which is harmless (cancel it).
- setup-python may not have a 3.11 build for 26.04 at first. That is a reason to stay pinned, and the canary will show it.
- P1 ignores full-line YAML comments on purpose, so WHY blocks can name the label they replace. One older comment in daily_sync.yml ("adds one ubuntu-latest runner-minute ...", above the notify-run-verdict job) was left as is, to keep the diff to labels only.
- P5 compares against dda1211 and is a proof for this commit. Once later commits change workflows, run it against the matching base or leave the variable unset (the default in CI, where P5 to P7 skip).
- The canary runs only six lean tests and one Node harness, not the whole suite, the contract lane or the production writers. It shows the image can host the repository's tools and offline tests, not that every scheduled writer will succeed.

## Operator steps, including arming and rollback

1. Merge before Monday 19 Oct 2026. Nothing needs arming. Read-back: on the next CI run, the "Set up job" log section of any job shows Runner Image `ubuntu-24.04`, and the `CI verdict` stays CLEAN.
2. Optional control run: Actions, then "Runner image canary (manual)", then Run workflow with runner_image=ubuntu-24.04. Expect green. This is the baseline to compare against.
3. When GitHub offers the label, dispatch the same workflow with runner_image=ubuntu-26.04 (the default). If it is red, stay pinned and fix or record the failing step. If it never starts, the label is not available yet; cancel the run.
4. Un-pin only after a green 26.04 canary. In one reviewed commit, change the labels, preferably to the explicit `ubuntu-26.04` rather than back to `ubuntu-latest`. In the same commit, set PINNED_LABEL in tests/test_runner_pin_ubuntu2404.py to the new label, update the canary's choice list and fallback (and CANARY_RUNS_ON and CANARY_CHOICES in the harness), and bump ci.yml with a WHY block.
5. Rollback: `git revert` this commit. Every label returns to `ubuntu-latest`, the canary file and the harness are removed, and the lean list loses the one line. No ENV, Variable or secret is involved.

## Owner decisions owed

- Confirm GitHub's announcement of the 19 Oct 2026 `ubuntu-latest` move and its target image (this cannot be checked offline), and merge before that date.
- After un-pinning, choose between an explicit label (recommended: `ubuntu-26.04`, re-pinned the same way) and going back to the floating `ubuntu-latest`. The harness enforces whichever explicit label PINNED_LABEL names.
- Decide when to dispatch the 26.04 canary, and whether the result is enough to un-pin or whether a longer run on 26.04 is wanted first (for example dispatching selected workflows from a branch with the labels flipped).
- Set a reminder for GitHub's eventual retirement of the `ubuntu-24.04` label.
- Related item (outside this commit): promoting the whole-suite CI lane to a required check was planned for about 19 Oct. It should be done only after this pin has landed, so its green week runs on a stable image.
