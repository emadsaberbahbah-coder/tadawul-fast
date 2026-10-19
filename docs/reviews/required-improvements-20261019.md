# Required improvements — 19 October 2026

## Outcome and scope

This change implements the code repairs confirmed by the 19 October review:
recover an interrupted Drive backup without claiming an unconfirmed upload,
audit all resolved production dependencies, persist learning outcomes before
publishing their calibration, and integrate the reviewed decision-output and
native completion fixes. It extends PR #795 on `9aac4356`; it preserves the
source-read relay already on main `4c9e2cb`.

The repository changes are distinct from live installation. This review has
not installed GAS sources, changed broker evidence, enabled learning audit
schedules, released an investment decision, or changed live account holdings.
Passing software tests does not certify today's recommendations.

## Repairs

| Area | Confirmed problem | Resulting behavior |
|---|---|---|
| Drive backup | The 19 October upload lost its TLS connection after export; the helper never queried its resumable session. | Status probes and bounded continuation use the same session and server-acknowledged byte range. Matching file/content metadata is required before copy or retention. |
| Dependency security | The actual advisory job reported seven Starlette findings and one unused NLTK finding; URL-pinned httpcore was skipped. | FastAPI 0.133.0 and Starlette 1.3.1 resolve together; unused NLTK is removed. An isolated auditor scans installed production distributions, and a separate receipt verifies exact inventory coverage. Dependency failure is visible; typing remains advisory. |
| Learning publication | An armed audit could publish calibration before saving its outcomes, ignore a failed save, or mistake a failed read for an empty store. | Acknowledged reads and persistence precede publication. Required failed or partial publications return nonzero. No audit schedule is enabled by this repair. |
| Learning evidence ages | A newly exported report could look current while its accepted outcomes and checkpoint publication remained old. | The offline report uses an explicit as-of date and separately reports recorded, matured, evaluated and published evidence, plus overdue target dates. |
| Portfolio accounting | Fractional custody, missing quote/cost values and post-fee cash could be misrepresented at the output boundary. | Preserve known fractional quantities, keep unknown values unknown, bound proceeds to actual custody, and retain exact cash accounting before display rounding. |
| Native decision refresh | HTTP-200 error envelopes or a deferred/unverified refresh could be mistaken for a completed cohort. | Validate renderer inputs before clearing a board and require typed completion tied to consumed source/run/publication evidence. |
| Native ledger | An incomplete priced subset could imply a complete aggregate cost. | Every contributing active lot must prove quantity, price and fee; a missing component withholds the aggregate rather than extrapolating it. |
| Price display | Small nonzero prices could disappear at a two-decimal display boundary. | The existing optional precision contract reaches backend and native price cells without changing eligibility, ranking, financial amounts or default arming. |

### Reused reviewed decision changes

The decision integration is the scoped delta from the reliability baseline
`2981d4781f0986e587f2d03d0f3856eb6e5c4394` to reviewed PR #792 head
`4c9007d8a1d6463b5f8e7ad44bb54d9b555ba3e8`, including PR #791 head
`afbfc13b10c28e4d51912f01f1bfafb33b27e878`. It does not replace the newer
workflow repairs in PR #795. CI enrollment and deployment pins are reconciled
against the resulting combined source.

The native chain is present as source only. Its mode remains off, it installs
no trigger on load, and its live caller/lock graph is unverified. Integration
also preserves the existing optional panel-preservation and price-precision
controls. It does not authorize automatic refresh, funding or release.

## Evidence and verification

The recorded overnight backup failure is
[run 39185293314, job 117846330450](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39185293314/job/117846330450).
The previous dependency findings are in
[run 39163216738, job 117780701910](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39163216738/job/117780701910).

The exact original upload helper reproduces the injected TLS failure. The
repaired helper and existing backup suites pass 120 tests. Decision integration
passes 157 targeted tests with 10 existing opt-in/golden skips. New negative
cases cover failed persistence, incomplete upload acknowledgements, missing
cost and source proof, ambiguous pool counters and unverified native completion.

The strict installed-dependency audit reports 132 resolved distributions,
zero known vulnerabilities and zero skipped distributions. Its target is
Python 3.11.17. The local auditor uses Python 3.12 only because the local
proxy rejected its Python 3.11 transport; this does not change the audited
target or skip a dependency. The full JSON report and matching production-only
inventory are preserved in `docs/evidence/dependency-audit-resolved-20261019.json`
and `docs/evidence/dependency-installed-20261019.json`.
The inventory covers the isolated production-requirements environment,
including its transitive distributions and the seeded pip, setuptools and
wheel. It excludes the separate auditor and subsequently installed test tools;
it does not inventory Render's operating system. The CI production environment
is unseeded, so its package count can differ; its own inventory must still match
its own audit exactly. The exact-coverage verifier independently confirms all
132 local entries, with no omissions or version mismatch.

Repository CI uses Python 3.11 with exact production pins. The full test suite,
framework/authentication compatibility, required workflow jobs and the new
complete dependency audit must be checked on the published candidate SHA.
The current candidate's GitHub run links and completion receipts belong in
the PR description; an older successful run is not evidence for a newer tree.

Local socket-denied execution is not a valid result for localhost server or
TestClient integration tests. Those tests require local loopback permission;
no test assertion or application threading path should be weakened to work
around that environment limitation.

The initial combined local suite completed with 5,944 passes, 382 subtests,
33 existing skips and three failures. Two failures were the auth harness's
assumption that the installed framework's reconstructed URL path remained a
stable historical oracle. The test-only correction keeps the full corpus,
uses an explicit Starlette 0.41.3 reference, requires the literal ASGI path,
and adds a deliberate URL-based auth-helper mutation that the checks reject.
The complete auth harness passes on both the old and new frameworks; the
historical reference matches all 34,185 checked baseline scopes. Production
auth code and allow/deny policy are unchanged.

The final affected compatibility group passes 335 tests with 10 existing
optional skips. Its exact command, framework versions and installed-inventory
scope are recorded in
[the dependency remediation evidence](../evidence/dependency-remediation-20261019.md).

The third failure was a backup test's stale exact version assertion. It is
updated from 1.5.2 to 1.5.3, while its optional local-copy behavior, bytes and
permissions assertions remain intact. These corrected cases and the final
history-row fixtures require verification on the final published tree; the
initial broad-run receipt is diagnostic evidence rather than final clearance.

## Remaining prerequisites for production acceptance

1. **Current v2 GAS source.** `Portfolio_Decision_v2`, `Top_10_Investments_v2`
   and the DP2 generators are not represented by the available authentic
   source. Their actionable-output gating, common FX basis and coupon/price
   profit separation remain unverified. See
   [the concrete source and repair prerequisites](../apps-script-source-prerequisites.md).
2. **Fresh broker evidence.** A real current capture and its reconciliation
   must pass the existing freshness and custody requirements. Re-stamping old
   account data or relaxing the gate would not fix this prerequisite.
3. **Learning outcomes.** The scheduled audit and calibration-write flags are
   currently disabled. Catch-up needs evidence from each actual target
   session; today's quote cannot be substituted for an older target date.
4. **Render installation.** The live service remains on main until the tested
   change is integrated and deployed. Pair build command
   `bash scripts/build_web.sh` with start command
   `bash -lc 'set -euo pipefail; source .venv/bin/activate; exec ./scripts/start_web.sh'`.
   Respect a configured custom `TFB_BUILD_VENV` in both commands. Confirm the
   deployed commit and health/readiness responses after installation.
5. **Final source-cohort acceptance.** Refresh the native pages from the same
   verified source publication, then run current coverage and decision audits.
   The overnight failed audit predates the later native Top-10 refresh; it
   cannot establish the post-refresh outcome without a new audit.
6. **Foreign listing identity.** PR #794 is intentionally excluded from this
   integration. A synthetic response for `VHM.VN` declaring `VHM.US`, NASDAQ,
   USD and price 250 passes the existing provider's base-ticker comparison.
   With the proposed suffix tables, the engine changes its metadata to
   HOSE/VND without changing that price, removing the existing currency
   quarantine. All four table-only tests pass in that unsafe state. This
   proves a guard gap, not that the actual affected workbook quotes are US
   instruments. A safe repair needs returned-instrument/currency evidence
   and provider-to-identity regressions; table defaults alone are insufficient.
   The proposed `.PH` addition also leaves the request normalizer producing
   `AC.PH.US`, so its provider request path is not established.

## Operating sequence

Review the exact combined diff and all actual CI job conclusions, integrate
one coherent candidate, apply the paired Render configuration, and verify the
new deployment. Install GAS changes only after the complete current source
and caller graph are reconciled. Capture genuine broker evidence and perform
the native refresh plus acceptance audits. Keep blocked/uncertified decisions
blocked until those requirements are satisfied.
