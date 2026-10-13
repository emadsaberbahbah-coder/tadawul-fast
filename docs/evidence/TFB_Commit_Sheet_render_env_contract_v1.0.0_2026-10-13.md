# TFB Commit Sheet - Render ENV contract generator (render_env_contract 1.0.0, Build Spec H-8)

Date: 2026-10-13 (Tuesday). Base: the P0-6 stacked branch head fbc4d07d (it carries the PR #762-#764 work: ci.yml v1.1.15, verify_deployment 1.0.57, main.py 8.14.4). Lane: repository tooling, documentation and CI only. No production module changes. ci.yml moves from v1.1.15 to v1.1.16. verify_deployment moves from 1.0.57 to 1.0.58 because it pins the new script.

## Why

Build Spec item H-8 asks for a Render ENV contract, docs/render_env_contract.md. Nothing in the repository mentioned H-8 or render_env_contract, and render.yaml does not exist either, even though the Procfile comments refer to it. So no file in the repo recorded which ENV keys the web service reads, what their code defaults are, or which keys must not be set.

The gap is visible on the live service. On 13 Oct 2026 the owner's /health showed app_version 5.111.0 while entry_version was 8.14.3. app_version is not a code constant. config.py reads TFB_APP_VERSION, then APP_VERSION, and the first one that is set wins (default 'unknown'). main.py maps '', 'unknown', 'dev', 'none' and 'null' to its APP_ENTRY_VERSION, so /health reports app_version equal to entry_version only when both keys are unset. The literal 5.111.0 does not appear in main.py, config.py, env.py or core/config.py. It appears only in the engine's own WHY history. So one of the two keys is still set to 5.111.0, either in the Render service's own Environment list or in a linked Environment Group. The Procfile names the group tfb-server. The fix is to delete the key, not to change code, and the contract has to record that both keys MUST be unset.

A hand-written contract would go stale at once. docs/flags.md already trails HEAD: it says 869 flags, and regenerating it today gives 874. So this item adds a generator that reads the code, and a harness that keeps the committed document equal to the generator output.

## Evidence

- config.py, in Settings.from_env: `app_version=_env_first("TFB_APP_VERSION", "APP_VERSION", default="unknown")` and `env=_env_first("TFB_ENV", "ENV", "APP_ENV", default="production")`.
- main.py: `_coerce_version` maps {'', 'unknown', 'dev', 'none', 'null'} to APP_ENTRY_VERSION. /health publishes `app_version` and `entry_version` side by side. `_load_settings` tries config, then env, then core.config. env.py reads APP_VERSION, then SERVICE_VERSION, then VERSION (default 'dev'), but only if config.py's loader fails. core/config.py reads APP_VERSION as well, as the third fallback.
- main.py gate tables are the ones /health shows. `_PF_GATE_ENVS` has 25 keys (pf_gates.portfolio_actions) and `_OB_GATE_ENVS` has 9 keys (pf_gates.opportunity_builder). `_DEPLOY_ENVS` holds the six RENDER_* keys in the /health "deploy" block. engine_gates comes from core/data_engine_v2.py `surface_gate_states()`, which has 28 entries.
- scripts/verify_deployment.py FLAGS has 71 entries. 13 of them are in `_WORKFLOW_SCOPED`, so 58 are Render-scoped.
- Procfile line 13 names the canonical env group tfb-server (UVICORN_*, WEB_CONCURRENCY, WORKERS_MAX).
- The read-only review behind this item is in the scratchpad file wfA/5_R-repo.md, section (5) and SKEPTIC amendment (5). That amendment asked for four things, all done here: the golden negative taken from dda1211 (where the doc is absent), an engine_gates section, auth-related keys kept out of the public doc while owner decision D7 is open, and no import of the scanned modules. It also advised against a blocking committed-doc equality test. This item keeps that test, as the task asked: the generator is deterministic, lives beside its inputs and runs in about 1.5 s. The failure message says how to regenerate. See Known limits and Owner decisions.

## What changed

1. New scripts/render_env_contract.py, SCRIPT_VERSION 1.0.0, with a dated WHY.
   - It uses the standard library and ast only. It never imports the modules it scans: each source file is read as text and parsed.
   - It never reads os.environ, so the document cannot depend on, or leak, the environment of the machine that runs it.
   - Defaults come from a small static evaluator. It walks a side-effect-free subset of Python and substitutes "unset" for every ENV read: the read returns its default argument or None, and os.environ behaves as an empty mapping. Anything outside that subset is shown as `-`, never guessed. Module constants are resolved from literals, and a dict that names another constant (portfolio_actions DEFAULT_CONTROLS) is evaluated on demand. Imports inside the repo are followed only by reading the target file; for example engine price_precision is read from core/price_precision.py.
   - It writes docs/render_env_contract.md with four sections:
     - A. Platform keys outside the TFB_ prefix.
       - TFB_APP_VERSION and APP_VERSION: MUST be unset. The section cites config.py's read order, main.py's mapping and the 13 Oct /health reading.
       - SERVICE_VERSION and VERSION: SHOULD be unset (env.py fallback only).
       - APP_ENV (unset, or production), plus TFB_ENV and ENV: SHOULD be unset, because config.py reads them before APP_ENV.
       - PORT, WEB_CONCURRENCY and REDIS_URL (name only).
       - The six RENDER_* deploy keys: supplied by Render, never set by hand.
       - Read order, mapping tokens and the files that read each key are derived by AST from a fixed file list.
     - B. Gates visible on /health.
       - B.1: every _PF_GATE_ENVS key.
       - B.2: every _OB_GATE_ENVS key.
       - Each row gives the reading function, the raw code default and the reader's return value when every ENV is unset (for example `_env_dd_guard_mode() -> 'off'`, `_env_enabled() -> True`).
       - B.3: every engine_gates key. Each row gives the ENV names it derives from (two call levels deep), their raw defaults and the /health value when unset. env_combo, sai_version and the three stats entries read no ENV and are labelled runtime state.
     - C. The 58 Render-scoped verify_deployment FLAGS entries. Each row gives the manifest default, the meaning, whether it is a kill switch (and a declared disarm), whether it is a value or a toggle, and whether it is also on /health.
     - D. A pointer to docs/flags.md. Nothing from that file is copied.
   - Every row has an "Expected production value" column. The values come only from an owner-filled config/render_env_expected.json, of the form {"recorded": "YYYY-MM-DD", "expected": {"NAME": "value"}}. That file is absent, so the preamble says "expected production value: not recorded" and every row says "not recorded". The MUST-be-unset rows always say MUST be unset; a value given for them in that file is ignored with a WARN. Any value that does not look like a plain flag value (an '@', '://', 24 or more opaque characters, or characters outside a plain set) is withheld.
   - Under owner decision D7, any name that matches the auth, token, secret or key pattern is withheld from all sections. None matched today: the document says so and names nothing.
   - The document contains no line numbers and no module version strings, so it does not change on unrelated edits.
   - CLI:
     - `python scripts/render_env_contract.py` writes the doc. This default mode is WARN-only: it exits 0 and prints a WARN when the content changed.
     - `--check` writes nothing. It exits 0 when the doc is current and 1 when it differs or is missing, and the message gives the regenerate command.
     - `--output PATH` writes elsewhere; `-` means stdout.
     - `--root DIR` and `--expected PATH` change the inputs.
     - Exit 2 means an input error, such as an unreadable expected-values file.

2. New docs/render_env_contract.md, generated (196 lines, 21030 bytes, ASCII).

3. New tests/test_render_env_contract.py, HARNESS_VERSION 1.0.0, with a dated WHY. It runs as a script (prints "PASS k/k", exits 0 or 1) and under pytest (one parametrized test per case). The cases:
   - R1: the generator imports only stdlib modules (checked against sys.stdlib_module_names). It builds the doc with socket connect, create_connection and getaddrinfo blocked, and imports none of the scanned modules.
   - R2: every _PF_GATE_ENVS and _OB_GATE_ENVS key, read from main.py by the harness's own AST code, has a section B row.
   - R3: the TFB_APP_VERSION and APP_VERSION rows say MUST be unset in both the contract and expected columns, and section A cites 5.111.0.
   - R4: every surface_gate_states key has a B.3 row. Every Render-scoped FLAGS entry has a C row, and no workflow-scoped entry does.
   - R5: no secret-looking value, URL or D7 key name appears. The harness keeps its own copy of the pattern.
   - R6: the doc is ASCII.
   - R7: the committed doc equals the generator output. On failure it says: "docs/render_env_contract.md is stale: regenerate with `python scripts/render_env_contract.py` and commit the result (first difference at line N)".
   - R8: the output is identical with APP_VERSION, TFB_APP_VERSION, two gates, REDIS_URL and PYTHONHASHSEED set to probe values, and no probe value leaks into it.
   - R9: CLI exit codes. --check gives 0 on the committed doc and 1 on a stale or missing copy. The default mode rewrites a stale copy with a WARN and exits 0.
   - R10: expected-values handling. With no file, the doc says "not recorded". With an owner file, a plain value is shown, a secret-looking value and a D7 name are withheld, and APP_VERSION stays MUST be unset.
   - R11: the golden negative. With TFB_RENDER_ENV_CONTRACT_BASE pointing at a dda1211 copy, R1, R2, R3 and R7 must each FAIL there (no generator, no doc). It skips when the variable is unset.
   - R12: a self-test of the R5 check. It must report four seeded secrets appended to an in-memory copy of the doc.

4. .github/workflows/ci.yml v1.1.15 -> v1.1.16 (a new header block with a dated WHY and the regenerate command):
   - tests/test_render_env_contract.py is enrolled in the blocking lean-unit step "Work-brief capture, repair and pure contract regressions", right after tests/test_flag_registry.py.
   - The push paths filter gains config/render_env_expected.json and docs/render_env_contract.md, so a push that edits either one runs the check itself instead of turning the next unrelated push red. pull_request was already unfiltered.
   - Nothing was removed. With those three added lines taken out, the parsed YAML equals the base's (checked with yaml.safe_load).

5. scripts/verify_deployment.py 1.0.57 -> 1.0.58, with a dated WHY.
   - SCRIPTS gains ('render_env_contract', '1.0.0', 'static Render ENV contract (H-8)'). New versioned scripts are pinned there. flag_registry is the precedent for a static doc generator, and 13 of the 15 versioned scripts added since 7 Oct are pinned: 12 in SCRIPTS and performance_math in MODULES. The two exceptions are smoke_endpoints and verify_core_repair_readback.
   - The script is import-safe for check_scripts: stdlib only, no module-level work, and main() behind `__main__`.
   - No FLAGS entry and no verifier logic changed. The AST def inventory is 14 -> 14: 0 removed, 0 added. tests/test_verify_manifest_pins.py passes.

6. This sheet.

## Default OFF identity

No production module changed: main.py, config.py, env.py, core/, routes/, integrations/ and apps_script/ are untouched. The web service behaves byte-identically and no Render ENV change is needed. The generator runs only when someone invokes it. The application never imports it, and verify_deployment imports it only to read SCRIPT_VERSION.

The new CI check cannot usefully be ENV-gated. A switch that defaults off would mean the contract is never checked. A new TFB_ variable to switch it on would grow the flag registry this review is trying to shrink, and it would need a GitHub settings change before it did anything. The check is therefore always on, like every other lean-lane harness. Its golden negative is the only part gated by a variable (TFB_RENDER_ENV_CONTRACT_BASE), and that part skips in CI.

The push-path additions change only when CI runs, not what it checks. The verify_deployment change adds one line to the verifier's SCRIPTS report. No other verifier output changes.

## Measured results with exact commands and outputs

Python is /home/user/tfb-venv/bin/python (3.11.17). B is a dda1211 source copy made with `mkdir -p $B && git archive dda1211 | tar -x -C $B`, where B=<scratchpad>/build/render-env-contract/base_dda1211.

`python scripts/render_env_contract.py --check`:

    [render_env_contract v1.0.0] OK: .../docs/render_env_contract.md is current
    rc=0

`python tests/test_render_env_contract.py`:

    PASS R1 generator is stdlib-only, offline and import-free | stdlib only (10 modules); offline; 196 lines
    PASS R2 every _PF/_OB gate key has a section B row | 25 _PF_GATE_ENVS + 9 _OB_GATE_ENVS keys present
    PASS R3 TFB_APP_VERSION and APP_VERSION say MUST be unset | TFB_APP_VERSION and APP_VERSION rows say MUST be unset
    PASS R4 engine_gates and Render-scoped FLAGS complete | 28 engine_gates rows; 58 Render-scoped FLAGS; 13 workflow-scoped kept out
    PASS R5 no secret-looking value or D7 key name | no secret-looking value, no URL, no D7 key name
    PASS R6 committed document is ASCII | 21030 bytes, all ASCII
    PASS R7 committed document equals generator output | docs/render_env_contract.md equals the generator output
    PASS R8 output independent of the environment | identical output with APP_VERSION, gates and REDIS_URL set
    PASS R9 CLI --check and WARN-only default | --check 0/1/1, default mode rewrites with WARN and exits 0
    PASS R10 expected values file handling | absent -> not recorded; plain shown, secret and D7 withheld, MUST be unset kept
    SKIP R11 base golden negative | TFB_RENDER_ENV_CONTRACT_BASE unset
    PASS R12 R5 check reports seeded secrets | 4/4 seeded secrets reported; committed doc clean
    PASS 11/11 (1 skipped: TFB_RENDER_ENV_CONTRACT_BASE unset)

`TFB_RENDER_ENV_CONTRACT_BASE=$B python tests/test_render_env_contract.py` ends with:

    PASS R11 base golden negative | 4/4 gate cases FAIL on the dda1211 copy
    PASS R12 R5 check reports seeded secrets | 4/4 seeded secrets reported; committed doc clean
    PASS 12/12

The four gate cases run directly against $B report:

    R1 FAIL scripts/render_env_contract.py is missing
    R2 FAIL AssertionError docs/render_env_contract.md is missing under $B
    R3 FAIL AssertionError docs/render_env_contract.md is missing under $B
    R7 FAIL AssertionError docs/render_env_contract.md is missing under $B

Under pytest, `python -m pytest -q tests/test_render_env_contract.py` gives "11 passed, 1 skipped in 11.72s". With the base variable set it gives "12 passed in 12.20s".

Drift proof. A probe key TFB_PF_PROBE_NEW_GATE was added to `_PF_GATE_ENVS` in a scratch edit of main.py, which was reverted afterwards. The harness then printed:

    FAIL R2 ... gate keys without a section B row: ['TFB_PF_PROBE_NEW_GATE']
    FAIL R7 ... docs/render_env_contract.md is stale: regenerate with `python scripts/render_env_contract.py` and commit the result (first difference at line 74)

`--check` printed DRIFT with the same regenerate command and exited 1.

Determinism. The output sha256 54ef7706...e4b4ae was identical in five runs:
- PYTHONHASHSEED 0, 1 and 12345, each with APP_VERSION=5.111.0 and TFB_PF_ENABLED=0 set;
- an `env -i` run with `-I`;
- /usr/bin/python3.11, 3.12 and 3.13 with `-I`.

Related existing tests, run with `python -m pytest -q` over tests/test_render_env_contract.py, test_flag_registry.py, test_verify_manifest_pins.py, test_ci_required_verdict.py, test_runner_pin_ubuntu2404.py, test_workflow_flag_mappings.py, test_main_health_pf_gates_v8140.py, test_verify_optional_rollout_flags.py and test_repository_workflow_audit.py: 62 passed, 4 skipped. `python tests/test_verify_manifest_pins.py` gives "[MANIFEST PIN GUARD] 3/3 PASS". `python scripts/audit_repository_workflows.py` gives "workflow audit: errors=0 warnings=2" (the same two pre-existing warnings).

ci.yml lean list. Every run step of the lean-unit job was executed locally from the YAML (with tfb-venv python and /opt/node22 node; the pip step was skipped):
- py_compile: rc 0;
- decision-layer suites: "3525 passed, 18 skipped, 2 warnings, 162 subtests passed";
- work-brief step, which includes this harness: "286 passed, 1 skipped";
- model and display basis: "99 passed";
- isolated harnesses: "14 passed" twice;
- the five node harness steps: all passing;
- test_maturation: rc 0;
- overall: ALL_RC=0.

Compile and ruff: `python -m compileall -q` of both new files is clean, and `ruff==0.16.8 check --select E9,F63,F7,F82 .` gives "All checks passed!".

Zero removals:
- verify_deployment.py: 14 defs before, 14 after, 0 removed.
- SCRIPTS: 0 entries removed, 1 added.
- ci.yml push paths: 0 removed, 2 added. The parsed YAML equals the base once the three added lines are removed.

## Known limits

- The document is a static contract, not a readback of Render. Whether APP_VERSION or TFB_APP_VERSION is set today, and in which place, can only be seen in the Render dashboard.
- R7 is a blocking drift gate. Anyone who changes an input must regenerate the doc in the same commit, or the lean lane goes red; the failure message gives the command. The inputs are:
  - main.py: `_PF_GATE_ENVS`, `_OB_GATE_ENVS`, `_DEPLOY_ENVS` and `_coerce_version`;
  - a read site or default of one of those keys in portfolio_actions or opportunity_builder;
  - surface_gate_states or a helper it calls;
  - verify_deployment's FLAGS, `_WORKFLOW_SCOPED`, `_VALUE_FLAGS` or DECLARED_DISARMS;
  - the read order in config.py or env.py;
  - a new reader of a section A key in the fixed file list;
  - config/render_env_expected.json.

  Line numbers and version strings are deliberately absent, to keep this churn down. The SKEPTIC amendment preferred a structural test only (see Owner decisions).
- The static evaluator covers a subset of Python. A reader that takes arguments, loops, logs or raises shows `-` in the "Effective when unset" column; today that is 6 of 34 B.1/B.2 rows, and the raw default then applies. Engine ENV names are followed two call levels deep within parsed files. env_combo is derived inside core/surface_action_invariants.py, which the engine reaches through a function-local import, so it is shown as runtime state.
- The D7 filter is a name pattern. It withholds nothing today. A future key that carries a secret under an unexpected name would be printed by name only, never with a value: values come only from the owner file, which withholds anything opaque.
- Section A's "Read by" scans a fixed file list (main.py, config.py, env.py, core/config.py, core/data_engine_v2.py, portfolio_actions, scripts/worker.py, scripts/start_web.sh), not the whole tree. Shell defaults in scripts/start_web.sh are referenced, not parsed.
- docs/flags.md is stale against this head (874 flags against 869 committed). It is not regenerated here. Its rows carry line numbers, so a regeneration is a large, conflict-prone diff on a stacked branch. The contract only points to it.
- tests/test_runner_pin_ubuntu2404.py P5 (base-dependent, skipped in CI) already fails at this stack head with TFB_RUNNER_PIN_BASE_DIR set, because the v1.1.14 and v1.1.15 lean enrolments changed ci.yml's parsed YAML. This item adds one more enrolment line. Not fixed here.
- After merge, until Render deploys the new commit, a verify_deployment run on the old pod reports render_env_contract MISSING. That is the normal manifest-ahead behaviour for every new pin.

## Operator steps (including rollback)

1. Review and merge. Nothing in Render needs changing for this commit. The push run of ci.yml enforces the new lean check.
2. Delete the stale version key in BOTH places, ideally batched with any other planned ENV edits. Saving an ENV change redeploys the service and empties its in-memory caches.
   - Service ENV: in the Render dashboard, open the web service, then Environment. If APP_VERSION or TFB_APP_VERSION exists, delete it and save.
   - Linked env groups: open Environment Groups, then tfb-server, and every other group linked to the web service. Delete APP_VERSION and TFB_APP_VERSION wherever they appear, and save.
   - While you are there, check that SERVICE_VERSION, VERSION, TFB_ENV and ENV are not set (the contract says SHOULD be unset).
3. Read-back after the redeploy. Make the same GET /health read that showed 5.111.0 on 13 Oct. app_version must equal entry_version: 8.14.3 on the build that is live today, or the main.py APP_ENTRY_VERSION of whatever build is live. If app_version still differs, a key remains in a place not yet checked (another linked group, or the service list); repeat step 2. The OpenAPI version string follows the same value.
4. Optional: fill config/render_env_expected.json with names and plain values only, never secrets. Then run `python scripts/render_env_contract.py` and commit the file and the regenerated doc together.
5. Rollback of this commit: `git revert <commit>`. That removes the generator, the doc, the harness and its ci.yml line together, along with the two push paths and the verify_deployment SCRIPTS entry. ci.yml returns to v1.1.15 and verify_deployment to 1.0.57. There is no production behaviour to roll back.
6. Rollback of the ENV deletion in step 2 (only if something turns out to depend on the old label): re-add the deleted key with its old value in the same place and save. Nothing in this repository reads app_version (daily_sync and smoke_endpoints read entry_version first). Only 3 of about 23 Apps Script files are mirrored here, though, so an unmirrored reader cannot be ruled out.

## Owner decisions

- D7 (public repository) is still open. Auth, token and secret key names stay out of the public contract until it closes. If the repository stays private, the owner may later allow those names (a generator change: version bump plus regenerate).
- Keep R7 (committed doc equals generator output) blocking, as shipped, or demote it to a WARN, as the review's SKEPTIC amendment suggested by analogy with docs/flags.md. Demoting means removing R7 from the blocking lean run, or making it advisory. Either way, `--check` stays available.
- Fill config/render_env_expected.json from the dashboard (names and plain values only), so the "Expected production value" column means something.
- Whether SERVICE_VERSION, VERSION, TFB_ENV and ENV should move from SHOULD be unset to MUST be unset.
- Whether to regenerate docs/flags.md in a separate commit once the stacked branches have merged.
- The Build Spec v1.4.3 text of H-8 is not in the repository. Confirm that sections A to D match its intended scope.
