# Workflow and deployment review — 14 October 2026

Reviewed repository `emadsaberbahbah-coder/tadawul-fast` at main commit
`f3a7ab152f1fc8d545cc451d19ff5eb93d32d21a`. Dates and times below use
Asia/Riyadh unless marked UTC. Render inspection was read-only in the
owner-confirmed workspace `My Workspace` (`tea-d3m0uuumcj7s73aatrpg`).
The patch was subsequently integrated with main `c588c51` after PRs #767, #768
and #769 merged, preserving the environment-contract generator, manifest pin,
blocking tests, default-off decision-feed cohort guard, and routed-path
authentication fixes.

Scope: all 36 workflow definitions and their action expressions, build/start
commands, deployment verifier, publication/recovery audit scripts, dependency
resolution and vulnerability reports, whole Python test discovery, and all 11
JavaScript test harnesses (12 after the cohort guard merged). Passing these
checks establishes tested behavior;
it does not certify the correctness of live market data or every production
integration. No production workbook, Apps Script, service setting, or deployment
was changed.

## Repairs included

| Severity | Reproduced problem | Result after repair |
| --- | --- | --- |
| P1 | Inline recovery ignored manual `backend_url` and `sheet_id` overrides and could replay writes into the default production workbook. | Primary sync and recovery resolve the same target before credentials and writes. |
| P1 | Final publication treated the literal inputs `null`/`undefined` as spreadsheet IDs while source writers fell back to the repository ID. | Final audit uses the same sentinel handling as its source writers. |
| P1 | Historical manual recovery silently used the default workbook/backend even when its source run used an override. | Dispatch requires explicit source `backend_url` and `sheet_id`; blank, sentinel, or multiline values fail before writes. |
| P1 | Branch dispatches and scheduled main writers had different concurrency groups for the same workbook. | Sync, historical recovery, and grid repair share the ref-independent `tadawul-production-write` lease. CI retains separate groups. |
| P2 | Historical recovery planned from original sync artifacts without overlaying successful inline recovery, so it could retry already refreshed pages. | It downloads the source run's recovery artifacts into the same deterministic final-evidence overlay used by the audits. |
| P1 | Grid repair embedded `targets_json` and confirmation inputs directly in shell source; apostrophes and command substitutions were interpreted as shell code. | Inputs are environment data and quoted array arguments; the tool's confirmation/refusal exit remains authoritative. |
| P1 | Git tracked `scripts/start_web.sh` as non-executable although Procfile executed it directly: clean checkout exited 126. | Launcher is tracked executable. Procfile no longer claims a nonexistent Blueprint makes it executable. |
| P1/P2 | `PORT=0` could bind an unintended port; zero worker cap disabled bounding; huge numeric settings escaped clamps; configured backlog was ignored; a subshell discarded the raised file limit. | Invalid ports fail before launch; worker/time/backlog settings are bounded; backlog reaches Gunicorn; the file limit survives exec within the host hard limit. |
| P1 | Provider audit returned `TOO_SMALL` before historical row-collapse detection: 200→10 rows stayed green. | Historical collapse precedes the small-sample exemption, including structural-zero pages. Failed measurements cannot replace accepted references. |
| P1 | A 50→49 observation could replace the measurable baseline, allowing a later 1-row sample to escape collapse checks. | Small samples preserve the accepted measurable reference, including when another page advances. The subsequent 1-row observation fails at 98% collapse. |
| P1 | Accepted provider baseline entries containing NaN, Infinity, negative/out-of-range shares or invalid row counts could certify corrupt control state. | Accepted shares must be finite 0–100%; rows must be positive integers. Malformed and enormous values return a control failure. |
| P1 | The deployment verifier accepted NaN numeric budgets because comparisons with NaN are false. | Strict verification rejects non-finite values as drift. |
| P2 | Verifier `--json` stdout included imported-module startup messages, and omitted the wiring evidence explaining drift. | Import diagnostics go to stderr; stdout contains one JSON document including wiring findings. |
| P2 | Render's current build command logs `grep: unrecognized option '--hash=sha256:[0-9a-f]{64}'`, ignores `pip check` failure, and traverses its virtual environment during compilation. | New executable `scripts/build_web.sh` installs the selected requirements, requires `pip check` to pass, and compiles source while excluding environments and scratch/cache trees. The dashboard still needs its build command updated. |

Provider audit is version 1.2.1, launcher 2.6.2, verifier 1.0.62, and CI 1.1.19.
New deployment, target-resolution, shell-input, and baseline regressions run in
the blocking lean CI lane. No audit threshold was lowered and no release gate
was armed.

## Render deployment findings

Service `tadawul-fast-bridge` (`srv-d4hnir15pdvs739bqe1g`) uses branch `main`,
one Pro instance in Frankfurt, and port 10000. Automatic deploy is `no` and its
trigger is `off`. The latest inspected deploy,
`dep-db79stqd0e5s73fqihe0`, started **14 October, 00:11:19** and finished
**00:12:55**. It successfully deployed older commit
`dda1211456d959413a8c16ce73f86942a6d7ee2a`; the latest eight inspected deploys
succeeded. There is no evidence of a failed deployment of reviewed main
`f3a7ab1`: that commit had not been deployed.

Public `/health` and `/readyz` returned HTTP 200, with engine ready, no missing
required keys, no failed modules, and global authentication enforcement enabled.
They reported entry version 8.14.3 and deployed commit `dda1211`, while the
initially reviewed main contained entry 8.14.4; the integrated security update
contains entry 8.14.5. `app_version=5.111.0` is a separate environment-facing
label; use the deploy commit and entry/engine versions for release identity.

Render's **Health Check Path is blank**, so its configured check does not verify
HTTP readiness. `/readyz` defaults to report-only compatibility behavior:
`TFB_READYZ_STRICT` and the `READY_REQUIRE_*` requirements must be chosen explicitly
before HTTP status can serve as a deployment gate. Public `ready=true` alone does
not establish that those stricter requirements were enabled.

Recommended configuration after merging reviewed fixes:

1. Finish existing writers before launching workflows with the new shared lease.
   Old in-progress runs and reruns retain their old workflow definitions and
   concurrency groups. Restrict the production environment to approved release
   refs; the new lease cannot rewrite old definitions.
2. Set the Render Build Command to `bash scripts/build_web.sh`. It defaults to
   `.venv`, prefers `requirements.render.txt` if present, otherwise
   `requirements.txt`, and supports explicit `TFB_REQUIREMENTS_FILE` and
   `TFB_BUILD_VENV` overrides.
3. Use the environment produced by the build in the Start Command, for example
   `bash -lc 'set -euo pipefail; source .venv/bin/activate; exec ./scripts/start_web.sh'`.
   A custom build environment needs the corresponding activation path.
4. Select the reviewed commit for a manual deployment. Automatic deployment is
   currently intentionally off; no setting was changed by this review.
5. Verify JSON engine, route and authentication state. Once the intended
   `READY_REQUIRE_ENGINE`, `READY_REQUIRE_ROUTES`, and `READY_REQUIRE_AUTH`
   checks pass, set `TFB_READYZ_STRICT=1` and Health Check Path `/readyz`.
   Validate optional-route behavior before making it a deploy blocker.

The local real-Gunicorn smoke used offline providers, synthetic open/test auth,
engine and route readiness requirements, strict `/readyz`, one worker, and a
localhost port. It returned healthy/ready with entry 8.14.4, engine 5.154.0,
zero failed routes, and clean SIGTERM exit. This validates local startup and
shutdown; it does not replace production authentication validation.

## Genuine live audit failures

Recent red workflows reached their audits successfully. These failures are
data/control findings, rather than missing dependencies or failed authentication:

| Control | Observed evidence | Required repair |
| --- | --- | --- |
| Global market coverage | 224 names missing among 6,609 rows: 96.6107% versus the 99% threshold. Acquisition coverage 95.4002%; 102 timestamps blank, unparseable, or date-only. | Repair actual names, acquisition and timestamps; rerun complete coverage. |
| Decision freshness | Portfolio decision 13 October 20:45:09 predates portfolio source 22:22:27. Top-10 20:37:17 predates market sources 22:25:56–23:24:04. | Refresh decisions after the completed source cohort. |
| Source consumption and membership | Final audit reports `PF_SOURCE_CONSUMPTION_UNPROVEN` and `T10_SOURCE_CONSUMPTION_UNPROVEN`; Top-10 claims 2,469 funds versus the exact approved 2,474 without native exclusion proof. | Deploy the corresponding native producers and verify real consumed-source and per-symbol exclusion receipts. Do not fabricate receipts or restore an obsolete roster. |
| Provider monitor | Restored accepted baseline is empty and dated 18 August 2026, 10:08:35: `CH_BASELINE_EMPTY`/missing page references. | After a healthy measured sync, explicitly bootstrap/accept the verified baseline and inspect readback. |

Physical approved membership itself passes: 255 leaders and 2,474 funds. The
final publication audit correctly returned `[2,2]`, published the
`NOT_ACTIONABLE(final_publication)` blocker, and did not promote the decision feed.
Healthy source acquisition, native receipts, current decisions and confirmed
readback must precede baseline acceptance or executable-feed release.

Evidence:

- [Read-only post-merge verification](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38469376585/artifacts/11850957444), checked 14 October 00:20:26; all three audits exited 2.
- [Final publication artifact](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38454099730/artifacts/11847251206).
- [Provider baseline failure](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38412531825/job/115348295329).

These observations are snapshots from the linked runs, not a claim that no later
operator repair occurred. No production Sheets/Apps Script credentials were
available to perform or verify those data repairs here.

## Existing security remediation

Independent vulnerability checks found **89 advisories in 13 direct packages**
at reviewed main. Auditing all 143 installed packages found no additional
transitive/test-tool advisories. Python 3.11 resolution and auditing of the full
closure for [PR #766](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/766)
confirmed **8 remaining advisories** after its compatible dependency updates.
That existing PR should be reviewed and merged rather than duplicated here.
Its conflicting CI header was resolved by merging current main `2add3b4` into
the existing security branch (commit `aad228f`), preserving all five security
change files, Render/runner checks and cohort guard. The newer-pin integration
passed 45 focused Python tests and 20 cohort Node checks, with 6 optional skips
in each; all 143 installed packages remained compatible. This updates the
proposed PR, not main or the live deployment.

Seven remaining advisories are in Starlette 0.41.3 and need a coordinated FastAPI
upgrade: current FastAPI metadata requires Starlette `<0.47.0`, while required
fixes extend beyond that range. Existing newer-framework testing records route
compatibility failures. The remaining NLTK advisory has no published fix; this
repository only checks availability and does not call the affected model APIs.

The Host-derived authentication-path fix in
[PR #769](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/769) merged
during the follow-up CI repair. Its scope-path checks and route version pins
are preserved in this integrated patch.
[PR #767](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/767), covering
the generated Render environment contract and stale `APP_VERSION` handling,
merged during this review and is preserved in the integrated patch base. The
dependency fixes remain a separate proposal.

Direct dependency pins resolve successfully, but transitive dependencies are
not locked. The advisory dependency workflow audits direct requirements and
skips URL-pinned httpcore; this review separately audited the resolved closure
and installed httpcore 1.0.9. Repository hygiene scanner findings were reviewed:
five secret hits are explicitly synthetic test fixtures, six high findings are
intended Markdown output strings, and 115 low findings are print statements.
The scanner's nonzero result is recorded rather than described as a clean scan.

## Validation

Frozen full patched-tree suite on the original reviewed base: **4,898 passed,
24 skipped, 273 subtests passed, zero failures**. The integrated tree also runs
the new environment-contract and affected deployment/workflow suites separately.

| Check | Result |
| --- | --- |
| Fresh Python 3.11.9 production requirements installation and 143-package compatibility check | Pass |
| Real canonical build in a fresh environment: pip install, pip check, source compilation | Pass |
| Blocking repair suites in a separate lean environment (numpy, pytest, PyYAML only) | 67 passed, 30 subtests passed |
| Integrated deployment/workflow/verifier/manifest/release suites after PR #767 | 127 passed, 10 skipped, 69 subtests passed |
| Integrated Render environment contract and generated-document check | 14 passed, 1 optional skip; document current |
| Final integration with PR #768: affected Python suites and all 12 Node harnesses | 238 Python tests passed, 14 skipped, 53 subtests passed; all Node harnesses passed |
| Authentication integration with PR #769: pinned contract lane and affected deployment/workflow checks | 412 passed, 19 optional skips, 127 subtests passed; auth and repair changes preserved |
| All workflow action/schema/expression checks, actionlint 1.7.12 | Pass; external shellcheck/pyflakes integrations disabled |
| Python compilation and ruff E9/F63/F7/F82 across the repository | Pass |
| All JavaScript harnesses, including the newly merged cohort guard | Pass |
| Shell syntax and whitespace/error checks | Pass |
| Provider control self-test | 16/16 passed |
| Decision freshness self-test | 6/6 passed |
| Repository workflow audit | 0 errors; 2 pre-existing temporary force-refetch warnings |
| Real offline Gunicorn engine/route readiness and graceful shutdown | Pass |

Three existing pytest warnings concern legacy tests returning values. Temporary
`TFB_SYNC_FORCE_REFETCH_SYMBOLS` overrides require operator verification before
removal; this review did not clear them automatically.
