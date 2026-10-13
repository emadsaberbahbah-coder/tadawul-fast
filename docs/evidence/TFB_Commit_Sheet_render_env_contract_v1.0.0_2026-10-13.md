# TFB Commit Sheet - Render ENV contract generator (render_env_contract 1.0.0, Build Spec H-8)

Date: 2026-10-13 (Tuesday). Base: the P0-6 stacked branch head fbc4d07d (it carries the PR #762-#764 work: ci.yml v1.1.15, verify_deployment 1.0.57, main.py 8.14.4). Lane: repository tooling, documentation and CI only. No production module changes. ci.yml moves from v1.1.15 to v1.1.16. verify_deployment moves from 1.0.57 to 1.0.58 because it pins the new script.

Amended the same day by a second commit (review fixes, same unreleased versions: render_env_contract 1.0.0, harness 1.0.0, ci.yml v1.1.16, verify_deployment pin unchanged). The section "Review fixes" below lists each finding and what happened to it; the rest of this sheet describes the code as amended.

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
       - PORT (Render injects it, default 10000; a value in tfb-server is optional, as scripts/start_web.sh lists it there), WEB_CONCURRENCY and REDIS_URL (name only).
       - The six RENDER_* deploy keys: supplied by Render, never set by hand.
       - Read order, mapping tokens and the files that read each key are derived by AST from a fixed file list.
     - B. Gates visible on /health.
       - B.1: every _PF_GATE_ENVS key.
       - B.2: every _OB_GATE_ENVS key.
       - Each row gives the reading function, the raw code default and the reader's return value when every ENV is unset (for example `_env_dd_guard_mode() -> 'off'`, `_env_enabled() -> True`).
       - B.3: every engine_gates key. Each row gives the ENV names it derives from (up to three call levels, following a module the engine imports inside a function), their raw defaults and the /health value when unset. env_combo is an ENV-combination check: the engine's `_sai_env_combo_state` gets core/surface_action_invariants.py from `_sai_probe` (a function-local import) and calls its `env_combo_violations()`, which reads TFB_ROW_SANITY_QUARANTINE, TFB_SURFACE_ROW_SANITY, TFB_T10_BLOCKED_INVARIANT and TFB_SURFACE_BLOCKED_INVARIANT. Its row lists those four names with /health value 'ok' when unset, and a "Cross-key rules" note under the table prints the rule rendered from the code: ROW_SANITY_REQUIRES_BLOCKED_INVARIANT holds when (TFB_ROW_SANITY_QUARANTINE or TFB_SURFACE_ROW_SANITY on) and not (TFB_T10_BLOCKED_INVARIANT or TFB_SURFACE_BLOCKED_INVARIANT on). sai_version and the three stats entries show "none found statically (see source)"; by reading the source they are runtime state (the invariant module's version and cache counters), but the document claims only what the static walk found.
     - C. The 58 Render-scoped verify_deployment FLAGS entries. Each row gives the manifest default, the meaning, the kill-switch status, whether it is a value or a toggle, and whether it is also on /health. A paragraph explains the kill-switch column: verify_deployment scores a kill switch DRIFT whenever it reads disarmed on the live service, unless it is in DECLARED_DISARMS, and a toggle reads armed on 1, on, true or yes (its `_ARMED`). Each kill-switch cell says "armed when unset", "unset reads DISARMED: set an armed value in Render", "declared disarm: reported, not scored", or (for a value flag) "armed only when set". Today two rows say unset reads DISARMED: TFB_ADV_ENGINE_CALL_TIMEOUT and TFB_OPP_BUILD_OFFLOOP, both with manifest default 0.
     - D. TFB_ and TRACK_ flags: a pointer to docs/flags.md (nothing is copied). It also says which names neither file covers: the non-TFB names read by main.py (`_load_settings` and the helpers it calls) and by the gunicorn launcher scripts/start_web.sh (its header lists the keys of the tfb-server group).
   - The preamble now says what the document covers (section A keys, the /health gates, the Render-scoped verifier flags) and that it is not an inventory of every ENV name the service reads.
   - Every row has an "Expected production value" column. The values come only from an owner-filled config/render_env_expected.json, of the form {"recorded": "YYYY-MM-DD", "expected": {"NAME": "value"}}. That file is absent, so the preamble says "expected production value: not recorded" and every row says "not recorded". The MUST-be-unset rows always say MUST be unset; a value given for them in that file is ignored with a WARN. A *_URL, *_URI or *_DSN name (today REDIS_URL) never shows its value: it says "recorded (value withheld)". Any other value is shown only if it looks like a plain flag value: 1 to 64 characters from letters, digits and `_ . , + - / %` (so no space, ':', ';' or '='), not 24 or more opaque characters, no secret shape (key block, URL, provider key prefix, cloud key, GitHub or Slack token, Google key, JWT), and no run of 16 or more characters mixing letters and digits. Everything else says "(withheld: not a plain flag value)".
   - The finished text then goes through the same secret checks the harness applies in R5 (the generator keeps its own copy of the patterns). If any matches, the script prints "refusing to emit the document" and exits 2, writing nothing and printing nothing to stdout, so a leak is stopped before it can be committed.
   - Under owner decision D7, any name that matches the auth, token, secret or key pattern is withheld from all sections. None matched today: the document says so and names nothing.
   - The document contains no line numbers and no module version strings, so it does not change on unrelated edits.
   - CLI:
     - `python scripts/render_env_contract.py` writes the doc. This default mode is WARN-only: it exits 0 and prints a WARN when the content changed.
     - `--check` writes nothing. It exits 0 when the doc is current and 1 when it differs or is missing, and the message gives the regenerate command.
     - `--output PATH` writes elsewhere; `-` means stdout.
     - `--root DIR` and `--expected PATH` change the inputs.
     - Exit 2 means an input error, such as an unreadable expected-values file, or a finished document that would carry a secret-looking value.

2. New docs/render_env_contract.md, generated (206 lines, 23679 bytes, ASCII).

3. New tests/test_render_env_contract.py, HARNESS_VERSION 1.0.0, with a dated WHY. It runs as a script (prints "PASS k/k", exits 0 or 1) and under pytest (one parametrized test per case). The cases:
   - R1: the generator imports only stdlib modules (checked against sys.stdlib_module_names). It builds the doc with socket connect, create_connection and getaddrinfo blocked, and imports none of the scanned modules.
   - R2: every _PF_GATE_ENVS and _OB_GATE_ENVS key, read from main.py by the harness's own AST code, has a section B row.
   - R3: the TFB_APP_VERSION and APP_VERSION rows say MUST be unset in both the contract and expected columns, and section A cites 5.111.0.
   - R4: every surface_gate_states key has a B.3 row. Every Render-scoped FLAGS entry has a C row, and no workflow-scoped entry does.
   - R5: no secret-looking value, URL or D7 key name appears. The harness keeps its own copy of the pattern.
   - R6: the doc is ASCII.
   - R7: the committed doc equals the generator output. On failure it says: "docs/render_env_contract.md is stale: regenerate with `python scripts/render_env_contract.py` and commit the result (first difference at line N)".
   - R8: the output is identical with APP_VERSION, TFB_APP_VERSION, two gates, REDIS_URL and PYTHONHASHSEED set to probe values, and the unique sentinel carried by every probe value ("tfbprobe7f3c") never appears in it. It no longer tests for the bare word "probe", which a legitimate FLAGS meaning could contain.
   - R9: CLI exit codes. --check gives 0 on the committed doc and 1 on a stale or missing copy. The default mode rewrites a stale copy with a WARN and exits 0.
   - R10: expected-values handling. With no file, the doc says "not recorded". With an owner file, a plain value is shown; a secret-looking value, a D7 name and six leak probes are withheld (REDIS_URL "redis-host.internal:6379", "hunter2:pw", a "-----BEGIN" block, a spaced 36-character opaque string, a 20-character letters-and-digits run, "k=v"); REDIS_URL says "recorded (value withheld)"; R5 finds nothing in the output; and APP_VERSION stays MUST be unset.
   - R11: the golden negative. With TFB_RENDER_ENV_CONTRACT_BASE pointing at a dda1211 copy, R1, R2, R3, R7, R13 and R14 must each FAIL there (no generator, no doc). It skips when the variable is unset.
   - R12: a self-test of the R5 check. It must report four seeded secrets appended to an in-memory copy of the doc.
   - R13: the env_combo row lists every ENV name that core/surface_action_invariants.py `env_combo_violations()` reads (found by the harness's own AST walk, independent of the generator; four today), shows 'ok' when unset, and the doc prints one rule line, with its ENV names, per violation code in that function; no B.3 row claims "no ENV read". It is not pinned to today's count, so a new rule needs a regenerate, not a harness edit.
   - R14: section C explains the kill-switch column, and every Render-scoped kill switch whose manifest default is not in the verifier's `_ARMED` (and is not a declared disarm) says "unset reads DISARMED"; the others say "armed when unset".
   - R15: the generator's own guard reports each R12 seed, and with render_markdown patched to return a seeded document, `main()` exits 2 in file mode (no file written) and in stdout mode (nothing printed), with a "refusing to emit" message.

4. .github/workflows/ci.yml v1.1.15 -> v1.1.16 (a new header block with a dated WHY and the regenerate command):
   - tests/test_render_env_contract.py is enrolled in the blocking lean-unit step "Work-brief capture, repair and pure contract regressions", right after tests/test_flag_registry.py.
   - The push paths filter gains config/render_env_expected.json, docs/render_env_contract.md, config.py and env.py. All four are generator inputs or its output, so a push that edits only one of them runs the check itself instead of turning the next unrelated push red. (main.py, core/**, scripts/** and tests/** were already in the filter.) pull_request was already unfiltered.
   - The v1.1.16 header block carries a dated review-fix note for config.py and env.py and a merge note: the dt10-cohort-binding-2026-10-13 branch also claims v1.1.16 on the same base fbc4d07d, so whichever branch merges second renumbers its header block to v1.1.17 and keeps both WHY blocks. Only the header conflicts; the two branches touch different lean-lane lines.
   - Nothing was removed. With the five added lines taken out, the parsed YAML equals the base's (checked with yaml.safe_load).

5. scripts/verify_deployment.py 1.0.57 -> 1.0.58, with a dated WHY.
   - SCRIPTS gains ('render_env_contract', '1.0.0', 'static Render ENV contract (H-8)'). New versioned scripts are pinned there. flag_registry is the precedent for a static doc generator, and 13 of the 15 versioned scripts added since 7 Oct are pinned: 12 in SCRIPTS and performance_math in MODULES. The two exceptions are smoke_endpoints and verify_core_repair_readback.
   - The script is import-safe for check_scripts: stdlib only, no module-level work, and main() behind `__main__`.
   - No FLAGS entry and no verifier logic changed. The AST def inventory is 14 -> 14: 0 removed, 0 added. tests/test_verify_manifest_pins.py passes.

6. This sheet.

## Review fixes (second commit, same day)

A review of the first draft raised nine findings. Each was re-checked against the code; seven are fixed, and the two ci.yml version findings (the same issue raised twice) are a merge-coordination item that this branch cannot fix alone.

1. env_combo labelled "runtime state (no ENV read)" (raised twice). Confirmed: env_combo_violations() reads four gate names through `_flag()`, and with TFB_ROW_SANITY_QUARANTINE=1 alone /health reports a VIOLATION. Fixed: the name walk follows function-local module imports, the row lists the four names with 'ok' when unset, the rule is printed under the table, and rows with nothing found say "none found statically (see source)". This sheet's B.3 sentence and Known limits are corrected.
2. R8 tested for the bare word "probe". Confirmed by replaying the experiment. Fixed: a unique sentinel.
3. config.py and env.py missing from the ci.yml push paths. Confirmed: both are generator inputs. Fixed: both added in the same v1.1.16 block, with a dated note.
4. ci.yml v1.1.16 collides with the dt10-cohort-binding-2026-10-13 branch (raised twice). Confirmed: same base fbc4d07d, same version, different header block. Not fixable from this branch. Recorded in the ci.yml header, in Known limits, in Operator steps (step 6) and in Owner decisions: whichever merges second renumbers its block to v1.1.17.
5. Section C did not explain "Kill switch". Confirmed: verify_deployment scores TFB_ADV_ENGINE_CALL_TIMEOUT and TFB_OPP_BUILD_OFFLOOP DRIFT when unset (manifest default 0, not declared disarms). Fixed: an explanatory paragraph and per-row cells; R14 checks them.
6. Coverage overstated and PORT contradicted start_web.sh. Confirmed: the preamble and section D claimed more than the document lists, and start_web.sh lists PORT in tfb-server. Fixed: narrowed wording, a "not covered" line naming main.py `_load_settings` and scripts/start_web.sh, and the PORT row now says Render injects it (default 10000) and a value in tfb-server is optional.
7. Owner-value filter weaker than R5. Confirmed with the reviewer's probe file. Fixed: name-only connection strings, a tighter plain-value rule, the R5 secret shapes applied per value, and a whole-document guard that exits 2 and writes nothing. R10 and R15 cover it.

## Default OFF identity

No production module changed: main.py, config.py, env.py, core/, routes/, integrations/ and apps_script/ are untouched. The web service behaves byte-identically and no Render ENV change is needed. The generator runs only when someone invokes it. The application never imports it, and verify_deployment imports it only to read SCRIPT_VERSION.

The new CI check cannot usefully be ENV-gated. A switch that defaults off would mean the contract is never checked. A new TFB_ variable to switch it on would grow the flag registry this review is trying to shrink, and it would need a GitHub settings change before it did anything. The check is therefore always on, like every other lean-lane harness. Its golden negative is the only part gated by a variable (TFB_RENDER_ENV_CONTRACT_BASE), and that part skips in CI.

The push-path additions change only when CI runs, not what it checks. The verify_deployment change adds one line to the verifier's SCRIPTS report. No other verifier output changes.

The review-fix commit touches only the generator, its harness, the generated doc, ci.yml (comments plus the config.py and env.py push paths) and this sheet. Its secret guard is not gated either, for the same reason as the check: it can only refuse to emit a document, and on today's inputs it fires on nothing, so the committed document is exactly the generator output (R7).

## Measured results with exact commands and outputs

Python is /home/user/tfb-venv/bin/python (3.11.17). B is a dda1211 source copy made with `mkdir -p $B && git archive dda1211 | tar -x -C $B`, where B=<scratchpad>/build/render-env-contract/base_dda1211. D is the first-draft commit 061f958e extracted the same way. All numbers below are from the tree as amended by the review-fix commit.

`python scripts/render_env_contract.py --check`:

    [render_env_contract v1.0.0] OK: .../docs/render_env_contract.md is current
    rc=0

`python tests/test_render_env_contract.py`:

    PASS R1 generator is stdlib-only, offline and import-free | stdlib only (10 modules); offline; 206 lines
    PASS R2 every _PF/_OB gate key has a section B row | 25 _PF_GATE_ENVS + 9 _OB_GATE_ENVS keys present
    PASS R3 TFB_APP_VERSION and APP_VERSION say MUST be unset | TFB_APP_VERSION and APP_VERSION rows say MUST be unset
    PASS R4 engine_gates and Render-scoped FLAGS complete | 28 engine_gates rows; 58 Render-scoped FLAGS; 13 workflow-scoped kept out
    PASS R5 no secret-looking value or D7 key name | no secret-looking value, no URL, no D7 key name
    PASS R6 committed document is ASCII | 23679 bytes, all ASCII
    PASS R7 committed document equals generator output | docs/render_env_contract.md equals the generator output
    PASS R8 output independent of the environment | identical output with APP_VERSION, gates and REDIS_URL set
    PASS R9 CLI --check and WARN-only default | --check 0/1/1, default mode rewrites with WARN and exits 0
    PASS R10 expected values file handling | absent -> not recorded; plain shown; secret, *_URL, leak probes and D7 withheld; MUST be unset kept
    SKIP R11 base golden negative | TFB_RENDER_ENV_CONTRACT_BASE unset
    PASS R12 R5 check reports seeded secrets | 4/4 seeded secrets reported; committed doc clean
    PASS R13 env_combo names its ENV inputs and rule | env_combo names 4 ENV inputs, 'ok' when unset; rule(s) printed: ['ROW_SANITY_REQUIRES_BLOCKED_INVARIANT']
    PASS R14 section C flags kill switches that read disarmed | 2 kill switch(es) flagged as must-set in Render: ['TFB_ADV_ENGINE_CALL_TIMEOUT', 'TFB_OPP_BUILD_OFFLOOP']
    PASS R15 generator refuses a secret-bearing document | 4/4 seeds caught; exit 2 with nothing written or printed
    PASS 14/14 (1 skipped: TFB_RENDER_ENV_CONTRACT_BASE unset)

`TFB_RENDER_ENV_CONTRACT_BASE=$B python tests/test_render_env_contract.py` ends with:

    PASS R11 base golden negative | 6/6 gate cases FAIL on the dda1211 copy
    ...
    PASS 15/15

The six gate cases run directly against $B report:

    R1  FAIL scripts/render_env_contract.py is missing
    R2  FAIL AssertionError: docs/render_env_contract.md is missing under $B
    R3  FAIL AssertionError: docs/render_env_contract.md is missing under $B
    R7  FAIL AssertionError: docs/render_env_contract.md is missing under $B
    R13 FAIL AssertionError: docs/render_env_contract.md is missing under $B
    R14 FAIL AssertionError: docs/render_env_contract.md is missing under $B

The new cases also fail on the first draft D, which is what the review found (R1, R2, R3 and R7 pass there):

    R10 FAIL REDIS_URL: owner value not withheld ('`redis-host.internal:6379`'); TFB_PF_TRUST_GATE: owner value not withheld ('`hunter2:pw`'); TFB_T10_SHOCK_PCT: owner value not withheld ('`-----BEGIN`'); ...
    R13 FAIL env_combo row lacks ['TFB_ROW_SANITY_QUARANTINE', 'TFB_SURFACE_ROW_SANITY', 'TFB_T10_BLOCKED_INVARIANT', 'TFB_SURFACE_BLOCKED_INVARIANT']; env_combo /health value when unset is '-', not 'ok'; ...
    R14 FAIL section C does not explain the Kill switch column; TFB_PA_PROTECT_SUKUK: kill cell 'yes'; ...
    R15 FAIL the generator has no secret guard

Runtime cross-check of the env_combo row and rule, on the scratch copy D (its core/ equals this head's), with `env -i PATH=/usr/bin:/bin <ENV> python -B -I -c "...import core.data_engine_v2 as de; print(de.surface_gate_states().get('env_combo'))"`:

    (nothing set)                                             -> ok
    TFB_ROW_SANITY_QUARANTINE=1                               -> VIOLATION:ROW_SANITY_REQUIRES_BLOCKED_INVARIANT: TFB_ROW_SANITY_QUARANTINE=1 is invalid while the W1A-1 blocked-invariant gate is ...
    TFB_ROW_SANITY_QUARANTINE=1 TFB_T10_BLOCKED_INVARIANT=1   -> ok

That matches the document: 'ok' when unset, and the rule holds only for row sanity on with the blocked invariant off.

Under pytest, `python -m pytest -q tests/test_render_env_contract.py` gives "14 passed, 1 skipped in 15.93s". With the base variable set it gives "15 passed in 17.23s".

R8 replay of the reviewer's experiment. In a scratch copy, the TFB_HORIZON_COHERENT meaning in scripts/verify_deployment.py was changed to end in " probe" and the doc regenerated. The first-draft harness then printed "FAIL R8 output independent of the environment | an environment value leaked into the document" and "FAIL 1/11 (PASS 10/11)"; the amended harness printed "PASS R8 ..." and "PASS 14/14". R8 still catches a real leak: with the generator patched to print os.environ APP_VERSION, it reports "FAIL R8 output independent of the environment | the document changes with the environment".

Owner-value probe (the reviewer's four values, two more leak probes and four plain values), `python -I scripts/render_env_contract.py --output - --expected exp.json`, rc 0. Expected-value cells: REDIS_URL "recorded (value withheld)"; "hunter2:pw", "-----BEGIN", the spaced 36-character string, "abcd1234efgh5678ijkl" and "k=v" all "(withheld: not a plain flag value)"; "observe", "BBD.US,NMM.US", 15.0 and "1" shown as plain values. A grep of the output for hunter2, BEGIN, redis-host, Zq8 and abcd1234 counts 0.

Drift proofs.
- A probe key TFB_PF_PROBE_NEW_GATE added to `_PF_GATE_ENVS` in a scratch edit of main.py (first commit): "FAIL R2 ... gate keys without a section B row: ['TFB_PF_PROBE_NEW_GATE']" and "FAIL R7 ... docs/render_env_contract.md is stale: regenerate with `python scripts/render_env_contract.py` and commit the result (first difference at line 74)"; `--check` printed DRIFT with the same command and exited 1.
- A second env_combo rule added in a scratch copy of core/surface_action_invariants.py (WARN_INVEST_REQUIRES_FETCHFAIL, on the warn-invest and fetchfail names): before regenerating, R7, R9 and R13 FAIL ("env_combo row lacks [...4 new names]; cross-key rule WARN_INVEST_REQUIRES_FETCHFAIL is not printed with its ENV names") and `--check` exits 1; after `python scripts/render_env_contract.py` the doc lists 8 names and prints both rules, and the harness gives "PASS 14/14" with no harness edit.

Determinism. The output sha256 1969b072f4ad209e...57ce903d was identical in seven runs: PYTHONHASHSEED 0, 1 and 12345, each with APP_VERSION=5.111.0, TFB_PF_ENABLED=0 and TFB_ROW_SANITY_QUARANTINE=1 set; an `env -i` run with `-I`; /usr/bin/python3.11, 3.12 and 3.13 with `-I`. It equals the committed doc's sha256.

Engine name walk depth. Raising MAX_CALL_DEPTH from two to three changed only the env_combo row's names, defaults and value: in a diff of the regenerated doc against the first draft, the only other B.3 change is the label of the four rows with nothing found ("none found statically (see source)").

Related existing tests, run with `python -m pytest -q` over tests/test_render_env_contract.py, test_flag_registry.py, test_verify_manifest_pins.py, test_ci_required_verdict.py, test_runner_pin_ubuntu2404.py, test_workflow_flag_mappings.py, test_main_health_pf_gates_v8140.py, test_verify_optional_rollout_flags.py and test_repository_workflow_audit.py: 65 passed, 4 skipped. `python tests/test_verify_manifest_pins.py` gives "[MANIFEST PIN GUARD] 3/3 PASS". `python scripts/audit_repository_workflows.py` gives "workflow audit: errors=0 warnings=2" (the same two pre-existing warnings).

ci.yml lean list. Every run step of the lean-unit job was extracted from the amended YAML and executed locally (with tfb-venv python and /opt/node22 node; the checkout, setup and pip steps were skipped):
- py_compile: rc 0;
- decision-layer suites: "3525 passed, 18 skipped, 2 warnings, 162 subtests passed";
- work-brief step, which includes this harness: "289 passed, 1 skipped" (re-run alone after the last harness edit: same result);
- model and display basis: "99 passed";
- isolated harnesses: "14 passed" twice;
- the node harness steps: final board funding 64 passed, native source readiness 73 passed, Apps Script syntax rc 0, reconciliation inputs 16 passed and input truth "20 passed, 3 skipped (dual-tree cases need --base)";
- test_maturation: rc 0;
- overall: ALL_RC=0.

Compile and ruff: `python -m compileall -q` of the generator and harness is clean; `ruff==0.16.8 check --select E9,F63,F7,F82 .` gives "All checks passed!", and `--select F,E9` on the two files gives the same.

ci.yml push paths, parsed with yaml.safe_load: ['main.py', 'config/approved_universe_scope_v1.json', 'config/render_env_expected.json', 'docs/render_env_contract.md', 'config.py', 'env.py', 'core/**', 'routes/**', 'scripts/**', 'integrations/**', 'apps_script/**', 'tests/**', 'requirements*.txt', 'pyproject.toml', '.github/workflows/**'].

Zero removals:
- verify_deployment.py: 14 defs before, 14 after, 0 removed (unchanged by the review-fix commit).
- SCRIPTS: 0 entries removed, 1 added.
- ci.yml against fbc4d07d: 0 lines removed; 5 non-comment lines added (4 push paths, 1 lean entry). The parsed YAML equals the base once those five are taken out.
- Review-fix commit against the first draft: generator defs and classes 61 -> 71 (0 removed; added _with_combo_unset, check_publishable, combo_rules, is_plain_value, kill_switch_cell, local_imports, module_bindings, module_returners, render_condition, secret_findings); harness 25 -> 34 (0 removed; added case_r13, case_r14, case_r15, _function_env_names and the _Capture stdout stand-in).

## Known limits

- The document is a static contract, not a readback of Render. Whether APP_VERSION or TFB_APP_VERSION is set today, and in which place, can only be seen in the Render dashboard.
- R7 is a blocking drift gate. Anyone who changes an input must regenerate the doc in the same commit, or the lean lane goes red; the failure message gives the command. The inputs are:
  - main.py: `_PF_GATE_ENVS`, `_OB_GATE_ENVS`, `_DEPLOY_ENVS` and `_coerce_version`;
  - a read site or default of one of those keys in portfolio_actions or opportunity_builder;
  - surface_gate_states or a helper it calls, including the env_combo rule function in core/surface_action_invariants.py;
  - verify_deployment's FLAGS, `_WORKFLOW_SCOPED`, `_VALUE_FLAGS` or DECLARED_DISARMS;
  - the read order in config.py or env.py (both now in the ci.yml push paths);
  - a new reader of a section A key in the fixed file list;
  - config/render_env_expected.json.

  Line numbers and version strings are deliberately absent, to keep this churn down. The SKEPTIC amendment preferred a structural test only (see Owner decisions).
- The static evaluator covers a subset of Python. A reader that takes arguments, loops, logs or raises shows `-` in the "Effective when unset" column; today that is 6 of 34 B.1/B.2 rows, and the raw default then applies. Engine ENV names are followed up to three call levels within parsed files, including a module that a function imports in its own body or receives from a function that imports and returns it. Raising the depth from two to three found no new name for any other row. A dynamic import (importlib with a computed name) or an attribute call on anything else is not followed; such a row would say "none found statically (see source)", which is a statement about the walk, not a proof that the entry reads no ENV.
- env_combo's /health value when unset ('ok') comes from statically evaluating `env_combo_violations()` (an empty list) together with the engine function's 'ok' constant, because the evaluator stops at the import probe. The rule text is rendered from the if-test in that function; a test that is not and/or/not over zero-argument ENV readers prints "a condition the generator cannot render; see the source".
- The kill-switch cells mirror check_flags for plain toggles only. A kill switch that check_flags compares by name has its own rule there and is reported as "own rule in check_flags; see source"; none is in that state today.
- The owner-value filter is shape-based. A short secret that looks like a word (for example a 7-letter password) would pass it; the D7 name filter and the instruction "plain values only" are the protection there, and secret values do not belong in that file at all.
- The D7 filter is a name pattern. It withholds nothing today. A future key that carries a secret under an unexpected name would be printed by name only, never with a value: values come only from the owner file, which withholds anything opaque.
- Section A's "Read by" scans a fixed file list (main.py, config.py, env.py, core/config.py, core/data_engine_v2.py, portfolio_actions, scripts/worker.py, scripts/start_web.sh), not the whole tree. Shell defaults in scripts/start_web.sh are referenced, not parsed.
- docs/flags.md is stale against this head (874 flags against 869 committed). It is not regenerated here. Its rows carry line numbers, so a regeneration is a large, conflict-prone diff on a stacked branch. The contract only points to it.
- tests/test_runner_pin_ubuntu2404.py P5 (base-dependent, skipped in CI) already fails at this stack head with TFB_RUNNER_PIN_BASE_DIR set, because the v1.1.14 and v1.1.15 lean enrolments changed ci.yml's parsed YAML. This item adds one more enrolment line and four push paths. Not fixed here.
- After merge, until Render deploys the new commit, a verify_deployment run on the old pod reports render_env_contract MISSING. That is the normal manifest-ahead behaviour for every new pin.
- ci.yml version collision: the dt10-cohort-binding-2026-10-13 branch also declares "# VERSION 1.1.16 (2026-10-13)" on the same base fbc4d07d. This branch cannot change that one. Whichever merges second gets a header conflict and must renumber its own block to v1.1.17, keeping both WHY blocks (see Operator steps and Owner decisions).

## Operator steps (including rollback)

1. Review and merge. Nothing in Render needs changing for this commit. The push run of ci.yml enforces the new lean check.
2. Delete the stale version key in BOTH places, ideally batched with any other planned ENV edits. Saving an ENV change redeploys the service and empties its in-memory caches.
   - Service ENV: in the Render dashboard, open the web service, then Environment. If APP_VERSION or TFB_APP_VERSION exists, delete it and save.
   - Linked env groups: open Environment Groups, then tfb-server, and every other group linked to the web service. Delete APP_VERSION and TFB_APP_VERSION wherever they appear, and save.
   - While you are there, check that SERVICE_VERSION, VERSION, TFB_ENV and ENV are not set (the contract says SHOULD be unset).
3. While in the service Environment list, compare against the contract's other rules. engine_gates env_combo must read 'ok' on /health: never arm TFB_ROW_SANITY_QUARANTINE or TFB_SURFACE_ROW_SANITY without TFB_T10_BLOCKED_INVARIANT or TFB_SURFACE_BLOCKED_INVARIANT. Section C's two "unset reads DISARMED" kill switches (TFB_ADV_ENGINE_CALL_TIMEOUT and TFB_OPP_BUILD_OFFLOOP) must be set to 1 for verify_deployment to report CLEAN; if they are unset today, either set them (an owner decision with behaviour impact, not part of this commit) or record them in DECLARED_DISARMS. This commit changes neither.
4. Read-back after the redeploy. Make the same GET /health read that showed 5.111.0 on 13 Oct. app_version must equal entry_version: 8.14.3 on the build that is live today, or the main.py APP_ENTRY_VERSION of whatever build is live. If app_version still differs, a key remains in a place not yet checked (another linked group, or the service list); repeat step 2. The OpenAPI version string follows the same value.
5. Optional: fill config/render_env_expected.json with names and plain values only, never secrets. Then run `python scripts/render_env_contract.py` and commit the file and the regenerated doc together.
6. Merge order with the dt10-cohort-binding-2026-10-13 branch: both bump ci.yml v1.1.15 to v1.1.16. Merge one, then rebase the other and renumber its ci.yml header block to v1.1.17 with its WHY intact; keep both blocks. If this branch merges second, its block becomes "VERSION 1.1.17 (2026-10-13) -- RENDER ENV CONTRACT HARNESS (BUILD SPEC H-8)" and this sheet's ci.yml version reads v1.1.17.
7. Rollback of this item: `git revert` both commits (the first draft and the review fixes), newest first. That removes the generator, the doc, the harness and its ci.yml line together, along with the four push paths and the verify_deployment SCRIPTS entry. ci.yml returns to v1.1.15 and verify_deployment to 1.0.57. There is no production behaviour to roll back.
8. Rollback of the ENV deletion in step 2 (only if something turns out to depend on the old label): re-add the deleted key with its old value in the same place and save. Nothing in this repository reads app_version (daily_sync and smoke_endpoints read entry_version first). Only 3 of about 23 Apps Script files are mirrored here, though, so an unmirrored reader cannot be ruled out.

## Owner decisions

- D7 (public repository) is still open. Auth, token and secret key names stay out of the public contract until it closes. If the repository stays private, the owner may later allow those names (a generator change: version bump plus regenerate).
- Keep R7 (committed doc equals generator output) blocking, as shipped, or demote it to a WARN, as the review's SKEPTIC amendment suggested by analogy with docs/flags.md. Demoting means removing R7 from the blocking lean run, or making it advisory. Either way, `--check` stays available.
- Fill config/render_env_expected.json from the dashboard (names and plain values only), so the "Expected production value" column means something.
- Whether SERVICE_VERSION, VERSION, TFB_ENV and ENV should move from SHOULD be unset to MUST be unset.
- Whether to regenerate docs/flags.md in a separate commit once the stacked branches have merged.
- Merge order of this branch and the dt10-cohort-binding-2026-10-13 branch (both claim ci.yml v1.1.16); the second renumbers to v1.1.17.
- TFB_ADV_ENGINE_CALL_TIMEOUT and TFB_OPP_BUILD_OFFLOOP: the verifier expects both armed in Render (kill switches with manifest default 0). Confirm they are set to 1, or declare them as disarms with a reason.
- The Build Spec v1.4.3 text of H-8 is not in the repository. Confirm that sections A to D match its intended scope.
