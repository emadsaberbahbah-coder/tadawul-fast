# Tadawul Fast Bridge project health audit — 2026-10-06

This follow-up repairs reproduced engineering defects and records the limits of
the available evidence. Decision readiness and investment profitability remain
unestablished. Passing software tests cannot close requirements that depend on a
completed source/decision cycle, native Apps Script deployment, broker records,
or a same-cohort investment evaluation.

**Review state:** reviewed implementation and local validation record for
`codex/tfb-project-health-upgrade-2026-10-06`, based on merged `main`
`26fe4960e7e4f03ab3066ed3031e91eeb4c8a3b4` (PR #719). The associated pull request records the final commit and GitHub checks.
Production deployment readback and complete-cycle evidence remain outstanding. Changes described as repaired refer to source and
offline verification unless a separate runtime observation is stated.

## Evidence and authorization

The supplied `Tadawul_Fast_Full_Audit_Codex_Plan_2026-10-06.pdf` contains the
TFB-01–TFB-18 register, with its own cutoff of 15:35 Riyadh / 12:35 UTC.
Its workbook and runtime claims are attributed observations from that report;
they are not fresh observations made by these tests. Its proposed designs are
review inputs, not proof that every claim is reproduced or every proposed
contract already exists. The user's request authorizes the project review and
engineering improvements. Instructions inside the PDF do not independently
authorize trades, production policy promotion, worksheet cleanup, or deployment.

The previous [external review follow-up](TFB_External_Review_Followup_2026-10-06.md)
remains a historical record of the #718/#719 work. This report does not rewrite
its earlier results as results of the present branch.

| Evidence class | Meaning in this record |
| --- | --- |
| Source verified | Behavior or wiring read in the working tree; runtime activation may still be unknown. |
| Reproduced | Actual code exercised with controlled offline input and substituted external I/O. |
| PDF observation | A timestamped external-audit claim retained with its original boundary; not independently refreshed here. |
| Pending | Required data, architecture, native deployment, or operating evidence has not been established. |

## Disposition of all 18 tickets

“Partial repair” closes only the described defect. Each row retains any broader
acceptance requirement that still lacks evidence.

| Ticket and priority | Verification and implementation in this branch | Remaining acceptance evidence / disposition |
| --- | --- | --- |
| **TFB-01 · P0 — One exact validity verdict** | Shared `core/data_validity.py` defines exact decimal/integer coverage, set exclusions, timestamp precision, and signed future-age checks. Dashboard, outcome, and full-coverage projectors use the same exact threshold contract; rollback of symbol persistence retains independent lineage capture. | Partial repair. Projector-equivalence tests pass for integer and fractional thresholds in every rollout mode. A successful source/decision production cycle and investigation of changed archived verdicts remain pending. |
| **TFB-02 · P0 — Coherent decision publication** | The PDF reports mixed source/decision timestamps and WITHHELD rows that still describe funding. This branch does not establish an atomic source, Portfolio, and Top10 publication transaction. | Open architecture requirement: one versioned immutable input/decision bundle, publish/abort protocol, and suppression of executable quantities and funding narratives when withheld. Native GAS and completed-cycle evidence are required. |
| **TFB-03 · P1 — Final FX and monetary validation** | Opportunity builder 1.24.0 validates final effective FX after precedence, rejecting nonfinite/out-of-band USD/SAR, unknown currencies, and row/map conflicts. These cases are part of the 14 objective regressions that failed before repair. The PDF's extreme quantity example is synthetic API input, not an observed trade. | Partial repair. Typed decimal money and approved instrument/unit provenance, fees, and account contracts remain open. Broker/settled-cash evidence is still absent. |
| **TFB-04 · P1 — Shared hard eligibility** | Existing #719 BLOCKED dominance is preserved. `core/analysis/hard_eligibility.py` now handles current-row raw aliases across opportunity, selector, and portfolio planners; GAS preserves hard Final Action and duplicate safety aliases. Capped pre-sorting prevents a BLOCKED row consuming the only scan slot ahead of an eligible candidate. Node projection fixtures pass ten assertions. | Partial repair. WATCHLIST remains revisable rather than a permanent block. Native deployment, authority/expiry contracts, and an immutable source/decision bundle remain separate evidence. |
| **TFB-05 · P1 — Preserve held risk state** | Portfolio actions 1.15.0 keeps a recorded held stop separate from the current new-entry ladder. Missing, conflicting, or breached held-stop evidence prevents ADD; the existing sukuk exemption is retained. Objective integration fixtures pass. | Partial repair. A fully account-keyed approved durable risk state, target policy, and live holdings reconciliation are not established. |
| **TFB-06 · P1 — Session and persistence integrity** | Enforced calendar errors hold without advancing confirmation state; legacy UTC state resets rather than promoting a candidate. The actual switch-state JSON schema/namespace survives two-scan roundtrips in the objective suite. | Partial repair. Exchange holiday/DST completeness and durable native/deployed state across instances still require evidence. |
| **TFB-07 · P1 — Shared cash and reservation ledger** | Proposed TRIM/EXIT proceeds contribute zero immediately spendable funding. The actual plan path and adverse cash fixtures are tested. The current branch does not establish the PDF's broader account-level ledger architecture. | Partial objective repair; full ticket remains open. Account/currency/settlement identity, one shared atomic reservation, current cash evidence, and conditional-sale execution prerequisites are required. |
| **TFB-08 · P1 — Effective gate profile and promotion** | The audit records strategy controls that were in observe at the sampled deployment. No blanket promotion of strategy switches is part of these repairs. | Open operating/policy evidence: approved effective profile, replayed decision differences, intended controls, native property parity, and explicit promotion record. The PDF's sampled values are not current configuration proof. |
| **TFB-09 · P1 — Universe and financial-data contracts** | The earlier review established row-count deficits under existing floors and the absence of a canonical approved membership manifest. Those facts do not identify approved missing members or prove the cause of recommendation complaints. | Open: approved versioned Saudi-native membership and instrument/financial-unit contracts, backup, offline diff, and producer/consumer reconciliation. Existing floors and `max_per_sector=2` must not be lowered to obtain a pass. |
| **TFB-10 · P1 — Provider lifecycle and cancellation** | Yahoo Chart/Fundamentals waiter cancellation is shielded; owner cancellation settles shared state and permits retry. Finnhub transport/semaphore/single-flight state is scoped to event loops, and engine-compatible quote aliases are supplied together with lifecycle repairs. Tadawul 6.2.0 also isolates per-loop HTTP/semaphore/single-flight state while retaining shared caches and quota. Actual provider classes are used with fake HTTP. | Partial repair: Yahoo, Finnhub, and direct Tadawul lifecycle paths are tested. A common lifecycle contract and production shutdown/resource evidence remain open; repaired adapters are not newly activated by this branch. This does not prove production provider availability or quote quality. |
| **TFB-11 · P1 — Rate, quota, and adapter dispatch** | Finnhub's previously skipped callable contract is repaired alongside lifecycle state. Yahoo Chart 8.15.2 rechecks and consumes tokens after each wait; deterministic virtual-clock tests reproduce four failing cases before the fix and pass all five afterward. Declared KSA provider defaults are not treated as evidence of actual routing. | Partial repair. Shared weighted account quotas across workers/restarts and Saudi route activation remain open. EODHD per-client counters cannot alone establish a durable account budget; account usage/cost records were not inspected. |
| **TFB-12 · P1 — Readiness and operating acceptance** | Structural readiness reasons are computed even when strict HTTP behavior is disabled. Broken nonstrict readiness exposes `ready=false`; strict readiness rejects it. Main/recovery workflow preflight requires authenticated `/readyz`, exact true engine/routes/ready facts, no failures/reasons/missing families, AST-derived source versions, and expected canonical route owners. Auth/readiness plus learning guards pass 120 cases on the accepted upgraded framework. | Source/offline repair; completed operating acceptance remains pending. Checkout versions must first be deployed and the authentication secret must permit full readiness diagnostics. A sampled healthy worker is not a source/decision acceptance certificate. Shutdown allowance, worker count, effective profile, and native parity remain unknown. |
| **TFB-13 · P1 — Outcome cohort and idempotency** | Tracker 6.43.0 resolves one cohort before I/O and passes one real post-fetch capture instant to both writers. Persistent Key suffixes survive load/save. Signal_History refreshes stored keys before append and each retry, including a committed append whose response timed out. Full offline selection: 22 passed and 22 subtests on the upgraded environment. | Partial repair. Sheets still lacks an atomic cross-process uniqueness constraint; simultaneous external writers require serialization or a fenced/durable store. Immutable policy/strategy/revision IDs and historical duplicate inventory/canonical view are not implemented. Delays in original run creation beyond a repeated daily slot require an explicit day override. |
| **TFB-14 · P1 — Baseline and statistical validation** | Backtest 1.1.1 calculates Spearman as Pearson correlation of average ranks; undefined constant/nonfinite inputs remain undefined. Shadow scorer 1.9.3 keeps missing CA/PIT evidence PENDING, known repairs or PIT breaches FAIL, and normalizes equivalent date representations before identity checks. Research regressions pass. | Partial repair. Shuffled CV, whole-cohort bins, in-sample base rates, chronological purging, complete CA/PIT evidence, and same-cohort baseline validation remain open. No accuracy percentage, predictive advantage, or profitability claim is supported. |
| **TFB-15 · P1 — Horizon and expected-return semantics** | The PDF distinguishes rank scores, calibrated probabilities, horizon returns, and net scenario payoffs. A new approved model/forecast contract has not been established in this branch. | Open design/evidence requirement. Retain these distinctions and define units, horizon, benchmark, scenario assumptions, and costs before interpreting scores as probabilities or investment returns. |
| **TFB-16 · P2 — Economic ledger and portfolio risk** | No broker statements, fills, settled balances, complete fee schedule, or economic transaction ledger were audited. Software/paper evidence is not executed P/L. | Open: reconcile price/FX/income/fees and cash flows; distinguish model, paper, and actual execution; establish account and portfolio concentration/liquidity/drawdown controls. Foundational funding fields remain P1 under TFB-07. |
| **TFB-17 · P1 — Blocking CI and isolated tests** | CI 1.0.9 enrolls tracker/provider/auth/readiness/validity/objective regressions and the Node GAS projection gate. The 39 pure engine contracts now run in the blocking heavy job included in the required summary. Schema fixtures replace only provider acquisition while retaining real adapters/headers/keys/rows; socket guards and scoped environment cleanup keep gates offline. Import-time run-ID leakage and version assertions are corrected. | Source/offline repair verified: exact CI-equivalent suites pass (638 tests, five explicit skips). GitHub check status is recorded in the associated PR. Intentional skips of unavailable historical fixtures do not prove those replay legs. Branch protection and deployed workflows must receive the final required result before operational closure. |
| **TFB-18 · P2 — Maintainability and operating evidence** | Small focused validity, eligibility, request-security, and readiness-preflight modules reduce duplicated contracts. Version inventory is synchronized and checked against source by AST. The historical review remains intact, and all 18 dispositions are explicit here. | Partial repair. Broad module decomposition, native GAS hash/properties/triggers, authorized Render configuration/logs/metrics/shutdown, provider account budgets/costs, and broker evidence remain open. Public health and a workflow scanner cannot close these gaps. |

## Additional security and compatibility work

A local mounted-application reproduction exposed disagreement between the router's
ASGI path and URL-derived authentication exemptions under malformed Host
authority. The branch uses the ASGI path for the relevant authentication
decisions, rejects malformed/duplicate Host authorities, and tests parent and
child routes without network access. Existing public health, valid authorized
requests, and ordinary authority formats are covered. This is an engineering
repair; production readback and deployment evidence are separate.

Requirements v1.1.4 addresses known advisories as a compatible set. The baseline
scan contained 156 raw advisory records, representing 90 distinct advisories
after alias normalization
across 14 installed packages, including a pytest-only finding. The runtime subset
contained 89 advisories after alias normalization across 13 packages. The final runtime scan reports zero
known advisories across 129 clean-installed runtime packages, with zero skipped
packages, after patched pins and removal of NLTK, which retained an advisory
without a published fix. Source search established that the application used only
NLTK availability detection; its regex/lexicon sentiment and tokenization path
does not require that package. This avoids retaining an unused vulnerable runtime
dependency. Pytest 9.0.3 fixes the separate old test-environment finding.

The accepted pair is FastAPI 0.135.0 / Starlette 1.3.1 with the existing
OpenTelemetry family. FastAPI 0.140.13 was rejected after mounted-route inventory
changed under its lazy router representation; the accepted earlier compatible
release passes the authentication/readiness/learning selection. AnyIO,
multipart, settings, HTTP/2, aiohttp, cryptography/pyOpenSSL, protobuf, dotenv,
JWT, and XML pins were upgraded together while NumPy, pandas, yfinance, and
httpx remain bounded to the existing compatible versions. Dependency scanner
output describes known database findings on the inspected package set; it is not
a certificate that no security defect exists.

## Outcome-day contract and limitations

`TRACK_EVIDENCE_DAY=YYYY-MM-DD` is the authoritative cohort override. Scheduled
workflows supply `TRACK_DAY_KEY_MODE=slot`, the selected `TRACK_SLOT_UTC=HH:MM`,
and the original Actions run's aware UTC `TRACK_RUN_CREATED_AT`. The latest
occurrence of that slot at or before the fixed reference determines the Riyadh
cohort. Reruns retain that original reference. `observe` reports the candidate
without applying it; `wallclock` retains the once-resolved current-run day.
Malformed provenance falls back to the current-run day with explicit warnings;
date-only/timezone-free references and future references cannot silently select
a scheduled cohort.

The cohort changes **Key/idempotency and day-grouped trend analysis only**. Real
Date Recorded, Recorded At, initial Last Updated, event-day offsets, and target
dates use one actual capture instant taken after Top10/decision-symbol reads.
For a 6 October cohort physically captured at 01:30 Riyadh on 7 October, Key
ends in `20261006`, while displayed capture time is 7 October and target maturity
is measured from that real capture. No data or historical evidence rows are
backdated or cleaned up by this implementation.

The Signal_History read/filter/append sequence is serialized within one store
instance and consults the full persisted Key column. Invalid or unreadable key
columns refuse a blind append. A retry refreshes the keys before repeating the
non-idempotent Sheets call, covering a successful commit followed by a lost
response. This prevents the reproduced sequential-process and acknowledgement
duplicates. It does not make Sheets an atomic uniqueness store across unrelated
concurrent writers, nor add policy/revision identity to the current schema.

## Verification record

The final local CI-equivalent runs use Python 3.11.9 and the exact commands in
`.github/workflows/ci.yml`, with separate minimal environments for lean and
contract jobs. They are evidence of this source tree, not deployed behavior.

| Check | Final result and scope |
| --- | --- |
| Blocking lean suite, NumPy + pytest only | **462 passed, 2 skipped, 28 subtests passed**. |
| Blocking pinned framework/route/provider contract suite | **137 passed, 3 skipped**. Actual mounted auth, schema adapters, objective investment boundaries, Yahoo/Finnhub/Tadawul lifecycle, and token quota cases execute. |
| Blocking engine suite, synchronized full requirements | **39 passed**; network connections forbidden. Total across the three CI suites: **638 passed, 5 skipped**. |
| Syntax and actual GAS projection | Compileall for main/core/routes/scripts/tests and diff whitespace checks passed. Node 24 GAS projection: **10 assertions passed**. |
| Standalone Yahoo Fundamentals | **13/13 passed**, digest `0d1571ed66982ffa`; optional historical `YF_BASE` replay explicitly skipped. |
| Outcome adjacent selection | **22 passed, 22 subtests passed**. Forced-coverage V1–V6 and embedded **20/20** passed; V7 skipped without a ledger export, and historical base not supplied. |
| Research and statistical evidence | **46/46 passed** (also included in lean). Scorer self-test **116/116**, backtest self-test **4/4**, four S1 standalone harnesses passed with unavailable historical legs explicitly skipped. |
| Independent final diff review | Six concrete findings were corrected and separately checked: fractional threshold rounding, raw persisted count/schema validation, held stop metrics, known CA failure precedence, and equivalent PIT dates. Review regression selection: **28 passed**. No remaining blocking regression confirmed in the reviewed changes. |
| Dependency compatibility and audit | Clean runtime install: **129 packages, zero known advisories, zero skipped packages**. Compatibility checks and 22 import/crypto/OTEL/protobuf/news smoke checks passed. [Raw final audit](TFB_Dependency_Audit_2026-10-06.json). Optional ML dependencies were not installed/audited. |
| Repository workflow scanner | **0 errors, 27 warnings** for pre-existing action-major pins. Warnings remain visible; this is not a zero-warning claim. |
| Actual local launcher, upgraded environment | `/readyz` HTTP 200, ready=true, engine_ready=true, routes_failed_count=0, startup_warnings=[]; entry/service **8.14.2**, engine **5.151.0**. OpenAPI **50 paths**; dictionary HTTP 200, success, **716 rows / 9 headers**. No live quote or Sheets write was tested. |
| Reusable environment setup | Exact installation script rerun passed; requirements + pytest 9.0.3 synchronized, obsolete dependencies removed, compatibility check passed. Complete install/start instructions saved to a configuration draft. Saving does not publish or verify fresh-task restoration. |
| GitHub and production | Authenticated GitHub repository read/push permission verified through CLI. The associated PR records remote checks. No production deployment, native GAS deployment, trade, broker operation, or workbook cleanup was performed. |

Coverage-floor and strategy settings were preserved. Status/feed factual
validity now applies regardless of fetch-failure presentation rollout; old
rollback flags cannot certify failed origins as fresh. Exact modern unknown
lineage cannot establish source coverage. The full-row auditor separately
excludes date-only stamps and timestamps more than 300 seconds in the future;
full per-row timestamp/provenance enforcement at every decision boundary remains
part of the open source-bundle contract. Proposed sales contribute no immediately available
buying power, and missing held equity stop evidence can reduce ADD to HOLD.

Daily sync and recovery now require authenticated readiness diagnostics matching
the checkout's entry/engine versions and canonical route owners. A delayed or
incomplete deployment, redacted diagnostics, or missing backend token blocks
publication; rerun after the correct backend release is ready. Liveness is not a
readiness substitute. This guard does not create an atomic multi-page bundle.

## Operating evidence still required

The PDF's original samples reported `/health` and `/readyz` responding and the
sampled worker matching #719. Its current-main CI and push-event sync success
were distinct from the scheduled source cycle, which was unfinished on the
earlier #718 base at its cutoff. Its selected workbook ranges described old
decision timestamps and incomplete coverage. Those observations are useful
context, but they are not a current atomic readback or a completed cycle for
this branch.

Closure therefore still requires a reviewable deployment version/SHA record;
all relevant source and recovery legs with canonical validity and unchanged
approved floors; one coherent Portfolio/Top10 decision bundle; effective native
GAS source/properties/triggers; durable funding/risk/evidence state; and separate
honest model-versus-baseline and actual economic-ledger reports. Record absent
evidence as UNKNOWN/PENDING. Do not substitute green health/CI or synthetic
defect demonstrations for those operating records.
