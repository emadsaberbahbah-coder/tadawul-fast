# TFB Commit Sheet - apps_script/16_Decision_Top10.gs v1.13.8 (P0-1 arming prerequisite 2: feed run bound to the source cohort)

Date: 2026-10-13. Base: the P0-6 stacked branch head fbc4d07d (ci.yml v1.1.15). The 16_Decision_Top10.gs at that head is byte-identical to origin/main dda1211 (sha256 2aec8a98a1f2fd77e8ba31695934d42bbc3a6646f1559448cec198fb96d1dc2d, DT10 v1.13.7). Item: P0-1 arming prerequisite 2, defence in depth for the audited decision-feed release of PR #763. Gate: Script Property DT10_FEED_COHORT_BIND, default absent (off).

Review fixes (2026-10-13): three reviewer findings were closed by a further commit on this branch. The delivered .gs is unchanged (same sha256, same paste). The harness is now VERSION 1.0.1 with a new case D5, tests/test_release_decision_feed.py is 1.1.2, and operator step 6 was rewritten. See 'Review fixes' at the end of this sheet.

## Why

With PR #763 (below this commit in the stack) the only writer of an EXECUTABLE 'TFB Decision Feed' value is scripts/release_decision_feed.py; the sync and the finalizer only downgrade. It writes exactly one value shape: 'EXECUTABLE | run=<SOURCE> | <anchor> | issued=... | expires=... | ttl_min=480 | release=... | attempt=... | receipts=T10:<SOURCE>,PF:<SOURCE> | ... | audits=0,0'. The release is bound to the audited cohort SOURCE, but the native cockpit was not. dt10BoardVerdict_ honoured an EXECUTABLE feed on its state and age only (dt10UvParse_), and never compared the feed's run= with the cockpit's own source cohort, which is the one run= shared by the four market _Status rows.

So a release for run A could render EXECUTABLE over a newer cohort B that no audit had checked. The writer-start supersede step of the release follow-up closes this only for writers that run a workflow definition containing the step while the release gate is armed. It does not cover a stale workflow definition (a re-run of a pre-merge run, or a dispatch from a ref without the step), a writer outside those workflows (Apps Script, a manual edit), a disarmed gate, or a run-listing race. The release commit sheet therefore lists, as arming prerequisite 2, a GAS cohort-binding check in dt10BoardVerdict_, gated by a Script Property, default off and byte-identical when off, with a node test on the real .gs. This commit builds it.

## Evidence

All line numbers are for the base file (dda1211, DT10 v1.13.7).

- apps_script/16_Decision_Top10.gs:1732-1747, dt10BoardVerdict_: the verdict is dt10UvParse_(dt10UvRead_(ss), Date.now()), then source readiness may downgrade it. Nothing reads the feed's run= token.
- apps_script/16_Decision_Top10.gs:7167 and :7196, dt10UvParse_: the head token must equal 'EXECUTABLE' and the first timestamp must be at most 480 minutes old (15 minutes of future skew). That is the whole test.
- apps_script/16_Decision_Top10.gs:2035-2038, dt10SourceReadinessCore_: each market _Status row must carry one positive run=<id>, and all four must agree. The shared value is out.cohort, the 'source readiness VERIFIED (run N)' receipt that dt10PoolNote_ (:2113) writes into Top_10 B2 (:6996).
- scripts/audit_decision_surface_freshness.py:99, SOURCE_RUN_RE (r"(?<![a-z0-9_])run\s*=\s*([^\s;|,]*)", re.I), and scripts/release_decision_feed.py:298, _single_run: this is how the Python release and its consumers count run tokens. The cockpit check uses the same rule, so superseded_by_run= is not a run token.
- docs/evidence/TFB_Commit_Sheet_release_decision_feed_v1.0.0_2026-10-13.md, Operator steps, prerequisite 2: the requested check, the reason text 'feed run X != cohort Y', the Script Property gate and the node test.

## What changed

apps_script/16_Decision_Top10.gs, DT10_VERSION 1.13.7 -> 1.13.8. The header Version line was bumped in lockstep, and a dated WHY block v1.13.8 (2026-10-13) was added above v1.13.7. ES5 only.

- dt10BoardVerdict_ keeps the raw feed string it already reads (one new local, feedRaw). After the existing source-readiness step it makes one guarded call. The guard is: the verdict is still EXECUTABLE, the feed was read (that is, DT10_UPSTREAM_VERDICT is not off), and dt10FeedCohortBindOn_() is true. Only then does it call dt10FeedCohortCheck_(feedRaw, source.cohort). On a miss the verdict becomes NOT_ACTIONABLE with the helper's reason, feed_cohort_blocked: true is set, and the helper's result is attached as feed_cohort. If anything in that block throws, the verdict becomes NOT_ACTIONABLE('feed cohort check failed').
- New var DT10_FEED_COHORT_BIND_PROP = 'DT10_FEED_COHORT_BIND'.
- New function dt10FeedCohortBindOn_ reads the Script Property. Only the exact word 'on' arms the check, after trimming and in any case. Absent, empty or any other word is off. A property-service failure reads as on (fail-closed), because the check can only withhold. The read is memoized once per execution through dt10PropMemoGet_ (P-202) and reset at refresh entry.
- New function dt10FeedCohortCheck_(raw, cohort) is a pure helper. It counts run tokens with the SOURCE_RUN_RE rule, emulated in ES5 without lookbehind. Its character classes spell out Python's str whitespace and the four non-ASCII letters that Python's case-insensitive [a-z] also matches, so the count equals release_decision_feed._run_tokens. Exactly one token is required. It must be a positive integer without a leading zero, and equal to the cohort, which must itself be a positive integer. Reasons:
  - 'feed run X != cohort Y'
  - 'feed run token missing/ambiguous' (no token, two or more tokens, or a non-numeric, empty or zero-led token)
  - 'cockpit source cohort unverified'
  - 'feed cohort check failed' (on a fault)
- dt10SelfTest gains two lines: 'feed cohort bind (DT10_FEED_COHORT_BIND): off|on' and 'feed cohort core: ok'.
- Census: functions 189 -> 191, vars 102 -> 103, zero removed. Against the base, the only non-comment lines that changed are 'var out;' -> 'var out, feedRaw = null;', the feed read line, and the version constant.

The check never upgrades anything to EXECUTABLE. It runs only after source readiness, so a source-blocked verdict keeps its 'source readiness: ...' reason, and the check does not even read the property in that case. The B2 receipt 'source readiness VERIFIED (run N)' comes from the readiness result, not from the feed, so it is untouched. A release can therefore still bootstrap in the order sync -> cockpit refresh (receipt) -> release -> cockpit refresh. A binding block is handled exactly like any other feed veto, for example final_publication. Selected becomes WITHHELD, the order surface and money are cleared, the allocation replay is not requested, the selection log rows are withheld with the reason (existing code path: dt10AppendSelectionLog_ calls dt10BoardVerdict_; the harness stubs the log itself), and _Status stays 'OK' because this is not a source block.

DT10_UPSTREAM_VERDICT=off remains the existing cockpit BYPASS. The feed is not read at all, so the binding is bypassed as well. Never use it as a kill switch.

Tests and CI:
- tests/js/dt10_feed_cohort_binding.test.js (new, VERSION 1.0.0; 1.0.1 after the review fixes) is a full-source vm harness, described under Measured results.
- tests/js/decision_input_truth.test.js: the version pins moved to 1.13.8, the census moved to 191/103, the designed lists gained the three names, T8 needs the two new self-test lines, and an armed self-test line check was added. The header carries a dated note.
- tests/js/decision_source_readiness.test.js: the version pin moved to 1.13.8.
- tests/test_release_decision_feed.py, docstring VERSION 1.1.1 (1.1.2 after the review fixes). G4 still requires byte identity for every other protected file. It now accepts a 16_Decision_Top10.gs that differs from its supplied dda1211 sibling only when the new harness passes against that same copy with nothing skipped (since 1.1.2: D5 included, and only after every other pair was compared).
- .github/workflows/ci.yml v1.1.15 -> v1.1.16 (the orchestrator renumbers when stacking). The harness was added to the lean job's 'Native reconciliation and complete calendar inputs' node step. Nothing was removed or moved.

Docs: docs/evidence/TFB_Commit_Sheet_release_decision_feed_v1.0.0_2026-10-13.md received line-level updates. Prerequisite 2 now reads 'BUILT in DT10 v1.13.8 (paste required, Script Property DT10_FEED_COHORT_BIND=on before arming the release)', with the original text kept after it. A dated 'Stacked follow-up' paragraph was added, along with parenthetical updates where the sheet called prerequisite 2 owed.

Not touched: dt10UvParse_, dt10UvRead_, dt10SourceReadinessCore_, the renderer and the orchestrator, every Python script, the workflows other than ci.yml, and scripts/verify_deployment.py. The verifier pins no Apps Script version, so there is no pin or SCRIPT_VERSION change.

## Default OFF identity

With DT10_FEED_COHORT_BIND absent, or set to any word other than 'on' (tested: '', off, OFF, blank, true, 1, yes, enforce, observe, onn, 'o n', 'on;', enabled), every existing function behaves as in v1.13.7. There are three exceptions:
- the version string;
- the two new dt10SelfTest diagnostic lines;
- one added memoized property read, made only while the parsed feed reads EXECUTABLE and the upstream verdict is on (zero reads for every other feed value and under the bypass).

The verdict object has the same keys and values, with no feed_cohort and no feed_cohort_blocked. This was proved against the dda1211 copy by harness D1-D3, deep-equal on:
- the full dt10BoardVerdict_ object, for 20 feed values times 14 off spellings, plus the bypass and a source block;
- the real renderer's titles, tables, writes, KPIs and stashed verdict;
- the real refreshDecisionTop10's POST bodies, _Status page-status arguments (the B2 line), selection-log arguments, stability calls, titles, tables, writes and feed reads.

The existing input-truth harness also proves the orchestrator off identity against v1.13.3 (T4, T11 with --base; it stubs the verdict). Gating is possible and used: this is a pure Apps Script change behind a Script Property.

## Measured results

Environment: worktree /home/user/wt/dt10-cohort-binding, node v22.22.0, Python 3.11.17 (/home/user/tfb-venv/bin/python). Base copies were made with git show into the scratch directory: dda1211:apps_script/16_Decision_Top10.gs (sha256 2aec8a98...) and, for the input-truth dual tree, 95bdac08^:apps_script/16_Decision_Top10.gs (v1.13.3, sha256 ce3ef1ea...). Delivered file sha256: 9d11832acae738be2299faa463ca6aa71b8e9a77ab31c05d4bf8d5e55bcd506c.

New harness, CI shape (no base):

    node tests/js/dt10_feed_cohort_binding.test.js
    ... 20 PASS lines (C1-C11), 6 SKIP lines (D1-D5, G1: DT10_BASE unset; 5 before the review fixes)
    dt10 feed cohort binding: 20 passed, 0 failed, 6 skipped
    PASS 20/20                                                   (exit 0)

What the 20 cases cover:
- C1: the property reader, the fail-closed outage, the memo, and read-only-when-EXECUTABLE.
- C2: the OFF corpus. Pinned v1.13.7 verdicts for 20 feed values times 14 off spellings: same run, other run, newer run, no token, two equal tokens, two different tokens, run=synthetic, run=local, zero-led, empty token, superseded_by_run only, release grammar for both cohorts, final_publication veto, supersede downgrade, stale (510 min), future (30 min), lower-case head, empty and no timestamp.
- C3: the ON corpus for 5 'on' spellings. Same run passes; 11 values are blocked with their reasons; non-EXECUTABLE verdicts are byte-identical on and off.
- C4: a source block keeps its reason and the property is not read.
- C5: the DT10_UPSTREAM_VERDICT=off bypass is identical with and without the binding, and the feed is not read.
- C6: faults. A property outage arms the check, a helper throw and a non-true ok withhold, a missing cohort withholds, and a feed read fault is unchanged.
- C7: the release grammar. Python compose_release_value and supersede_value equal the pinned fixtures; the release is accepted for its own cohort only; the supersede value never executes.
- C8: run= token parity with release_decision_feed._single_run on 57 values, including the Unicode case-fold letters and the whitespace characters where JS \s and Python \s differ.
- C9: the real renderer (armed other run is WITHHELD with money cleared; armed same run and the release value stay EXECUTABLE; off other run is EXECUTABLE).
- C10: the real refreshDecisionTop10. B2 keeps 'source readiness VERIFIED (run 90001)' and exactly one 'pool_reconciliation_v1=' token whether the board is withheld or executable.
- C11: helper edge cases, version lockstep, and ES5 and ASCII of the new code.

New harness, against the base copy:

    DT10_BASE=<scratch>/16_Decision_Top10.dda1211.gs node tests/js/dt10_feed_cohort_binding.test.js
    PASS D1 OFF identity vs base: full verdict objects deep-equal for every corpus value and off spelling
    PASS D2 OFF identity vs base: REAL renderer titles, tables, writes and payload verdict
    PASS D3 OFF identity vs base: REAL refreshDecisionTop10 (B2 status, POSTs, writes, selection log args)
    PASS D4 census vs base: zero functions or vars removed; added exactly the designed names; added code ES5 and ASCII
    PASS D5 designed regions only: delivered minus the v1.13.8 regions equals the base copy byte for byte
    PASS G1 GOLDEN NEGATIVE: every blocking armed case FAILS on the base copy (DT10_BASE)
    dt10 feed cohort binding: 26 passed, 0 failed, 0 skipped
    PASS 26/26                                                   (exit 0)   (25/25 before D5 was added)

G1 runs 13 armed cases against the base copy and requires each to fail with an AssertionError: the 11 blocking verdict values, the renderer other-run case and the refresh other-run case. All 13 fail on base, because base renders EXECUTABLE over the other cohort. The golden discriminates: with DT10_BASE pointing at a HEAD copy, G1 fails ('armed cases that unexpectedly pass on base') and D4 fails ('added exactly the designed names'). The run printed 'FAIL 23/25' and exited 1; since the review fixes D5 fails as well and the run prints 'FAIL 23/26' (exit 1).

Mutation checks (DT10_DELIVERED=<mutant>, each mutant killed, exit 1):
- downgrade removed: FAIL 14/20
- cohort hard-coded instead of the readiness cohort: 19/20
- first token accepted instead of exactly one: 17/20
- the cockpit's narrower _Status token rule: 18/20
- property failure fails open: 18/20
- case-sensitive 'on': 18/20
- check run under the bypass: 18/20
- helper fault re-promoted: 19/20
- JS \s and plain [a-z] in place of the Python classes: 19/20 (C8)

Existing node harnesses, all green:

    node tests/js/decision_board_funding.test.js        -> Full-source final board funding: 64 passed
    node tests/js/decision_source_readiness.test.js     -> Full-source native source readiness: 73 passed
    node tests/js/apps_script_syntax.test.js            -> Apps Script syntax: 3 sources compiled
    node tests/js/portfolio_reconciliation_inputs.test.js -> 16 native reconciliation input checks passed
    node tests/test_dt10_calendar_completeness.js       -> 25/25 PASS
    node tests/js/decision_input_truth.test.js          -> 20 passed, 3 skipped (dual-tree cases need --base)
    node tests/js/decision_input_truth.test.js --base <scratch>/16_Decision_Top10.v1133.gs -> Full-source input truth: 23 passed

Related Python tests:

    PATH=/opt/node22/bin:$PATH python -m pytest -q tests/test_release_decision_feed.py tests/test_daily_brief_blocked_inputs.py \
      tests/test_executable_quote_conflicts.py tests/test_approved_scope_native_binding.py tests/test_verify_manifest_pins.py \
      tests/test_ci_required_verdict.py tests/test_runner_pin_ubuntu2404.py
    186 passed, 16 skipped in 88.36s

Release harness goldens with dda1211 base copies. TFB_TEST_FINALIZE_BASE and TFB_TEST_SYNC_BASE point into one directory that also holds the dda1211 audits, the three .gs files, verify_deployment.py and the empty release_decision_feed.py, as that sheet documents:

    python -m pytest -q tests/test_release_decision_feed.py   -> 49 passed, 4 skipped (G7-G10 need TFB_TEST_SUPERSEDE_BASE_DIR)
    G4 printed: 'G4 16_Decision_Top10.gs: differs from base only by the gated v1.13.8 feed cohort binding (PASS 25/25)'
                'G4 byte-identical: 6 protected files'
    (since 1.1.2 the order is reversed and the message names D5; see Review fixes)

Before the G4 update, the same run failed G4 on the .gs digest. G4 also fails when the .gs sibling is a different file: with the v1.13.3 copy, the harness's D-cases fail.

ci.yml lean lane: every run step of the lean-unit job was extracted from the YAML and executed in order (python = the venv, node 22). All steps had rc=0:
- py_compile
- the lean pytest list: 3525 passed, 18 skipped, 162 subtests passed
- work-brief: 275 passed
- model/display basis: 99 passed
- the protective harnesses: 14 and 14 passed
- the five node steps, including 'dt10 feed cohort binding: 20 passed, 0 failed, 5 skipped / PASS 20/20'
- scripts/test_maturation.py

The YAML parses with yaml.safe_load. compileall and ruff (0.16.8, E9,F63,F7,F82) pass on tests/test_release_decision_feed.py.

## Known limits

- Built is not live. Nothing protects the board until the owner pastes v1.13.8 and sets DT10_FEED_COHORT_BIND=on. The repository cannot read the bound project's installed source or its Script Properties.
- The binding covers only the native cockpit (Top_10 via dt10BoardVerdict_). send_digest, run_daily_brief and tfb_acceptance do no cohort binding; they stop honouring a release at its expires=, as the release sheet says. The Portfolio_Decision cockpit lives in the out-of-repo 10_My_Portfolio.gs and is not changed here.
- A rerun attempt of the released run itself stamps the same run= on every page. The feed's run= and the cohort are then equal, so this check cannot see attempt 2. That case is covered by the writer-start supersede step (prerequisite 4), not by this check.
- The check compares with the readiness cohort, which is the run= shared by all four market rows. If the rows disagree, source readiness already withholds, with the source reason.
- Token counting follows SOURCE_RUN_RE. ES5 has no lookbehind and its \s and case folding differ from Python's, so the helper spells out Python's whitespace set (adds U+001C-U+001F and U+0085, drops U+FEFF) and the four letters U+0130, U+0131, U+017F and U+212A. A first draft that used JS \s and a plain [a-z] accepted 'run=5' directly preceded by U+017F, and 'run=5' directly followed by U+FEFF; Python rejects both. Parity on the 57-value corpus is exact. Characters outside that corpus are not enumerated.
- A property-service outage arms the check for that execution. With the release gate off no EXECUTABLE feed exists, so this has no effect there. With a release armed, it withholds only a feed whose run= is not the cohort.
- dt10SelfTest output gains two lines, the one deliberate change visible with the gate off.
- tfbMorningCockpitRefresh still logs the raw dt10UvParse_ state of the feed (log text only). The board it then renders uses dt10BoardVerdict_ and therefore the binding.
- The header Version line keeps its pre-existing em dash; only the number changed on that line. All other added text is ASCII.

## Operator steps

PASTE INSTRUCTIONS. This paste supersedes the owed v1.13.7 paste: the file carries every change up to v1.13.7 as well, so skip the v1.13.7 paste and paste v1.13.8 only.

1. In the Apps Script editor, open the file 16_Decision_Top10, select all, paste the whole delivered apps_script/16_Decision_Top10.gs (sha256 9d11832acae738be2299faa463ca6aa71b8e9a77ab31c05d4bf8d5e55bcd506c), and save.
2. Run dt10SelfTest. The log must start with '[DT10 v1.13.8] self-test' and must show 'feed cohort bind (DT10_FEED_COHORT_BIND): off' and 'feed cohort core: ok', plus every prior '... core: ok' line, with zero FAIL.
3. After the next scheduled sync completes, run refreshDecisionTop10 (or let the cockpit trigger run). Top_10 B2 must then show exactly one 'pool_reconciliation_v1=' token, and 'source readiness VERIFIED (run <that sync's run id>)' when sources are complete.
4. Leave DT10_FEED_COHORT_BIND unset until the release is armed. While the release gate is off no EXECUTABLE feed exists, so the property would have nothing to check.
5. When arming the release (the release sheet's arming list), set the Script Property DT10_FEED_COHORT_BIND=on as the last step before setting the repository variable TFB_DECISION_RELEASE=1. Re-run dt10SelfTest and confirm 'feed cohort bind (DT10_FEED_COHORT_BIND): on'.
6. After the first release, refresh the cockpit. B2 should read 'output: EXECUTABLE' for the released cohort. If instead the SELECTED title reads 'FEED NOT ACTIONABLE - feed run X != cohort Y' (the live title uses em dashes) and B2 reads 'output: WITHHELD', the check is working: the cell holds a live release of run X while the pages now carry cohort Y. (Rewritten by the review fixes; the earlier text said to release the current cohort instead, which cannot work while the old release is live.) Do not clear the board by deleting DT10_FEED_COHORT_BIND or by setting DT10_UPSTREAM_VERDICT=off: both would show run X's release as EXECUTABLE over cohort Y. Dispatching a release of Y does not help either: while the cell holds an unexpired EXECUTABLE for another run, release_decision_feed.py refuses it (prior class refused, reason executable_other_run, exit 3) and writes nothing. Do this instead:
   a. Retract the stale release with the release sheet's three-step kill: set the repository variable TFB_DECISION_RELEASE_KILL=1; dispatch 'Release Decision Feed (P0-1)' with mode=kill; read back the _Status 'TFB Decision Feed' cell and confirm it reads 'NOT_ACTIONABLE(final_publication) | run=X | <stamp> | release_killed | release=<kill run id>'. If the kill run shows as cancelled or the cell still reads EXECUTABLE, dispatch mode=kill again.
   b. Until that readback, send_digest, run_daily_brief and tfb_acceptance still honour run X's release, up to its expires=. They do no cohort binding (see Known limits); only the native Top_10 board is withheld. So do step a promptly.
   c. Set TFB_DECISION_RELEASE_KILL back to 0, or delete it. While it is set, every run of the release workflow takes the kill branch, mode=release included, and nothing can be released.
   d. Release cohort Y only in the normal order, once its receipts exist: Y's sync has completed, both cockpits were refreshed after it (Top_10 B2 reads 'source readiness VERIFIED (run Y)'), then dispatch mode=release with source_run_id Y, then refresh the cockpit and confirm 'output: EXECUTABLE'. After the kill the cell is a veto for run X. When X is older than Y, the release accepts it for Y (older_run_veto). If X is newer than Y (the pages were rewritten by an older run), the release of Y is refused with newer_run; wait for the next sync and release that cohort instead.

Rollback: delete the Script Property DT10_FEED_COHORT_BIND, or set it to anything other than 'on'. The next run is v1.13.7 behaviour. (Review fixes: if the board is withheld with 'feed run X != cohort Y' at that moment, deleting the property shows run X's release as EXECUTABLE again, so follow step 6 and retract first.) Alternatively, re-paste v1.13.7 (git show dda1211:apps_script/16_Decision_Top10.gs, sha256 2aec8a98a1f2fd77e8ba31695934d42bbc3a6646f1559448cec198fb96d1dc2d). Re-pasting v1.13.7 also removes the protection, so with a release armed, retract the release first, using the release sheet's three-step kill. Do not use DT10_UPSTREAM_VERDICT=off to withdraw a board: it is a bypass that forces EXECUTABLE and skips this check.

Remove from the repository: revert this commit. The release harness G4 then compares the .gs byte-for-byte again.

## Owner decisions

- GO for the paste. It supersedes the owed v1.13.7 paste.
- When to set DT10_FEED_COHORT_BIND=on. This sheet recommends: as the last step before arming TFB_DECISION_RELEASE, and never removed while a release can be live. The release sheet keeps prerequisite 2 not waivable until arming preconditions A and B hold.
- Whether the same binding should be built for the out-of-repo Portfolio_Decision cockpit (10_My_Portfolio.gs) and for the Python consumers (send_digest, run_daily_brief, tfb_acceptance). That is a separate item.
- Live reads this session cannot make: the installed DT10 version in the bound project, and the current Script Properties (DT10_UPSTREAM_VERDICT, DT10_FEED_COHORT_BIND).

## Review fixes (2026-10-13)

A reviewer raised three minor findings on the build commit. Each was re-checked on this branch, reproduced, and fixed with the smallest change. apps_script/16_Decision_Top10.gs is not changed: its sha256 is still 9d11832acae738be2299faa463ca6aa71b8e9a77ab31c05d4bf8d5e55bcd506c, so the paste, the self-test line and the rollback stay as above. Only tests and docs changed, so there is no new behaviour to gate and the default-off identity is untouched.

### Why and evidence

1. G4 hid drift in two protected files when node was missing. G4 of tests/test_release_decision_feed.py ran the .gs harness inside its loop over the protected pairs. With no node, pytest.skip ended the case before 11_Manual_Refresh_Coordinator.gs and 24_Sync_Dispatch.gs were compared. Reproduced with a dda1211 base directory whose 24_Sync_Dispatch.gs has one appended line: with node G4 failed on 24_Sync_Dispatch.gs, and with node_binary patched to return None it skipped, so the drift was not reported.
2. Operator step 6 asked for an action that cannot work. It said to 'release the current cohort instead'. While the cell holds an unexpired EXECUTABLE for another run, the release refuses: classify_prior(<release value for run 90000>, '90001') returns ('refused', 'executable_other_run'), and run() (scripts/release_decision_feed.py, the kill, sweep and classification steps) downgrades only an expired or malformed prior before it refuses with exit 3 and no write. Only a kill, a sweep after expiry or a writer's supersede step clears an unexpired other-run release. The step also never said to retract the old release, which send_digest, run_daily_brief and tfb_acceptance keep honouring until its expires=. After a kill the cell reads 'NOT_ACTIONABLE(final_publication) | run=90000 | 2026-10-12 13:00:00+03:00 | release_killed | release=777002', and classify_prior of that value for '90001' returns ('eligible', 'older_run_veto').
3. G4 and harness D4 claimed more than they checked. D4 compared removed lines and added function and var names only. A copy of the delivered file with one unrelated behavioural line added to dt10SourceCount_ (`if (String(value) === '424242') return 9007199254740991;`) passed the harness 25/25 against the dda1211 copy. G4 would have accepted it and printed 'differs from base only by the gated v1.13.8 feed cohort binding'.

### What changed

- tests/test_release_decision_feed.py, docstring VERSION 1.1.2 with a dated WHY. G4 now compares every other protected pair first, prints the byte-identical count, and runs the .gs proof after the loop, so a missing node skips only that proof. The proof also requires the harness's 'PASS D5 ' line. Its message now reads 'differs from base only inside the designed v1.13.8 feed cohort binding regions (harness D5; PASS k/k)'. No case was added or removed.
- tests/js/dt10_feed_cohort_binding.test.js, VERSION 1.0.1 with a dated WHY. New case D5 (runs only with DT10_BASE, like D1-D4 and G1). It removes the designed v1.13.8 regions from the delivered file, undoes the designed one-line edits, and requires the result to equal the base copy byte for byte. The regions are the header WHY block, the guarded block in dt10BoardVerdict_, the two doc comments with the var and the two new functions, and the two dt10SelfTest lines. The one-line edits are the header Version line, DT10_VERSION, 'var out' and the feed read line. Each region is also shape-checked, so code cannot sit in a region outside its designed place:
  - The WHY block must be comment lines with no comment close.
  - The dt10BoardVerdict_ block must be comment lines and then one 'if (out.state === 'EXECUTABLE' && feedRaw !== null) {' whose body is indented and never closes the brace early.
  - The new-functions region may hold exactly the var and the two functions at top level, with no other top-level code. Run alone in a fresh vm, it must define exactly DT10_FEED_COHORT_BIND_PROP, dt10FeedCohortBindOn_ and dt10FeedCohortCheck_.
  - The self-test region must be exactly two report.push(...) statements with no assignment or function in them.
  D5 also proves that it rejects four synthetic mutants: the reviewer's dt10SourceCount_ line, a statement after the guard, a top-level statement among the new functions, and an assignment in the self-test lines. The skip list gained 'D5 designed regions only'. No helper, case or assertion was removed.
- This sheet: the review-fix note under the header, line-level updates to the What changed and Measured results lines that described the harness and G4, operator step 6 rewritten (see Operator steps), and this section.
- docs/evidence/TFB_Commit_Sheet_release_decision_feed_v1.0.0_2026-10-13.md: the harness note in the 'Stacked follow-up' paragraph now names G4 v1.1.2, the compare-others-first order and D5.
- Not touched: apps_script/16_Decision_Top10.gs, .github/workflows/ci.yml (the harness is already enrolled at v1.1.16, and no new harness file was added), every Python script, and scripts/verify_deployment.py.

### Measured results

Environment as above. The base copies are the same: <scratch>/16_Decision_Top10.dda1211.gs (sha256 2aec8a98...), and a dda1211 base directory relbase (its three .gs files were checked equal to dda1211 with cmp).

Harness, CI shape and against the base copy:

    node tests/js/dt10_feed_cohort_binding.test.js
    dt10 feed cohort binding: 20 passed, 0 failed, 6 skipped
    PASS 20/20                                                   (exit 0)

    DT10_BASE=<scratch>/16_Decision_Top10.dda1211.gs node tests/js/dt10_feed_cohort_binding.test.js
    PASS D5 designed regions only: delivered minus the v1.13.8 regions equals the base copy byte for byte
    dt10 feed cohort binding: 26 passed, 0 failed, 0 skipped
    PASS 26/26                                                   (exit 0)

Golden negative, with DT10_BASE pointing at a HEAD copy: D4, D5 and G1 fail. The output is 'FAIL 23/26' (exit 1), and D5 reports 'line 4: delivered-minus-regions " * Version: 1.13.7 ..." vs base " * Version: 1.13.8 ..."'.

Finding 3 mutant (DT10_DELIVERED=<HEAD plus the dt10SourceCount_ line>, DT10_BASE=dda1211 copy): it printed 'PASS 25/25' before the fix. It now prints 'FAIL 25/26' (exit 1), with D5 reporting 'a change outside the designed v1.13.8 regions: line 1813: delivered-minus-regions "  if (String(value) === '424242') return 9007199254740991;" ...'. A scratch runner that loads the D5 helpers from the harness and applies them to mutant files gave:

    delivered 16_Decision_Top10.gs                         ACCEPTED
    dt10SourceCount_ line (finding 3)                      REJECTED (differs) line 1813
    'out.ageMin = 0;' after the guard                      REJECTED (shape) binding block guard close
    'dt10SourceCount_ = function ...' after the new var    REJECTED (shape) top-level code of the new region
    "(DT10_VERSION = 'x') +" in a self-test push           REJECTED (shape) no assignment or function in the self-test lines
    ' */ DT10_UV_TTL_MIN = 1; /*' in the WHY block         REJECTED (shape) WHY region is comment only
    'var unused = 1;' at the top of dt10SelfTest           REJECTED (differs) line 7519
    the dda1211 and v1.13.3 copies as "delivered"          REJECTED (shape) WHY block: expected exactly one line

Finding 1, with the reviewer's script (scratch/build/dt10-review/g4_nonode.py: G4 run with node present, then with node_binary patched to None):

    base dir with 24_Sync_Dispatch.gs drifted by one line:
      node present: FAIL apps_script/24_Sync_Dispatch.gs
      node absent:  FAIL apps_script/24_Sync_Dispatch.gs        (was SKIP before the fix)
    clean dda1211 base dir:
      node present: 'G4 byte-identical: 6 protected files', then
                    'G4 16_Decision_Top10.gs: differs from base only inside the designed v1.13.8 feed cohort binding regions (harness D5; PASS 26/26)', PASS
      node absent:  'G4 byte-identical: 6 protected files', then SKIP 'node is required to prove the DT10 v1.13.8 default-off identity'

Release harness:

    TFB_TEST_FINALIZE_BASE=<relbase>/finalize_decision_publication.py TFB_TEST_SYNC_BASE=<relbase>/run_dashboard_sync.py \
      python -m pytest -q tests/test_release_decision_feed.py                -> 49 passed, 4 skipped (G7-G10 need TFB_TEST_SUPERSEDE_BASE_DIR)
    same env, python tests/test_release_decision_feed.py                     -> PASS 49/49  (skipped 4)
    same, with the base dir holding the v1.13.3 .gs (relbase_wrong) -k g4    -> G4 FAILS (D5 'line 4 ... Version: 1.13.7 ... vs base ... 1.13.3')
    DT10_BASE=<relbase_wrong>/16_Decision_Top10.gs node tests/js/dt10_feed_cohort_binding.test.js -> D1-D5 and G1 FAIL, 'FAIL 20/26' (exit 1)
    python tests/test_release_decision_feed.py (no base env)                 -> PASS 43/43  (skipped 10)

Related suites:

    PATH=/opt/node22/bin:$PATH python -m pytest -q tests/test_release_decision_feed.py tests/test_daily_brief_blocked_inputs.py \
      tests/test_executable_quote_conflicts.py tests/test_approved_scope_native_binding.py tests/test_verify_manifest_pins.py \
      tests/test_ci_required_verdict.py tests/test_runner_pin_ubuntu2404.py
    186 passed, 16 skipped in 93.41s
    node tests/js/decision_board_funding.test.js          -> Full-source final board funding: 64 passed
    node tests/js/decision_source_readiness.test.js       -> Full-source native source readiness: 73 passed
    node tests/js/apps_script_syntax.test.js              -> Apps Script syntax: 3 sources compiled
    node tests/js/portfolio_reconciliation_inputs.test.js -> 16 native reconciliation input checks passed
    node tests/test_dt10_calendar_completeness.js         -> 25/25 PASS
    node tests/js/decision_input_truth.test.js            -> Full-source input truth: 20 passed, 3 skipped (dual-tree cases need --base)
    node tests/js/decision_input_truth.test.js --base <scratch>/16_Decision_Top10.v1133.gs -> Full-source input truth: 23 passed

ci.yml lean lane: every run step of the lean-unit job was extracted and executed in order (the same runner as above), and all steps had rc=0:
- py_compile
- the lean pytest list: 3525 passed, 18 skipped, 162 subtests passed
- work-brief: 275 passed
- model/display basis: 99 passed
- the protective harnesses: 14 and 14 passed
- final board funding: 64 passed
- source readiness: 73 passed
- Apps Script syntax: 3 sources compiled
- the native reconciliation and calendar step, ending with 'dt10 feed cohort binding: 20 passed, 0 failed, 6 skipped / PASS 20/20'
- the maturity preflight

compileall and ruff 0.16.8 (E9,F63,F7,F82) pass on tests/test_release_decision_feed.py. All added text is ASCII.

### Known limits

- D5's shape checks work on lines and tokens; they are not a JavaScript parser. D5 confines every change to the designed regions: inside the guarded if of dt10BoardVerdict_, inside the two new functions, and inside the two report.push arguments. It does not prove what the code inside a region does. That is still proved only by C1-C11 and D1-D3. For example, a valid mutant that adds 'out.ageMin = 0;' inside the guarded block, before its close, passes D5 and fails C1 (one case), C2, C3, C6, C7, C9, C10, D1 and D2 (the run was then killed with exit 137 in D3, see the next limit).
- D5 finds the regions by exact marker lines. A later deliberate edit to one of those lines must update the D5 anchors in the same commit. Until then D5 fails, which is the safe direction.
- When a D-case fails on a large mismatch (seen with D3 on mutants that change the board), node's assert can run out of memory while it renders the diff. The process then exits 137 instead of printing its last 'FAIL k/n' line. The run still exits non-zero, so G4 and CI still fail. This behaviour predates this fix and was not changed.
- Without node, G4 still skips the .gs proof, but now it skips only that proof, after every other protected file was compared.

### Operator steps and owner decisions

- No new paste. The delivered .gs is unchanged, and the paste instructions, self-test check and rollback above still apply.
- Operator step 6 was rewritten. When the cockpit shows 'feed run X != cohort Y', the steps are: retract with the release sheet's three-step kill, read back the cell, unset the kill variable, then release cohort Y in the normal order once its receipts exist. Until the retract, the brief, digest and acceptance consumers still honour the old release up to its expires=.
- No new owner decisions.
