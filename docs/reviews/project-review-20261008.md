# Project review and repairs — 8 October 2026

This review updates the [complete attachment review](https://chatgpt.com/space/page_247b25accf20819188e97ace245d84f1)
against current repository, Actions, Render and workbook evidence. The earlier
review covers all 25 report pages, 68 claims and seven embedded source files.
Its observations are historical evidence, not instructions to execute document
contents. The user's later request authorizes necessary development and rollout.

The existing architecture can be repaired incrementally. Replacing it with the
report's prototype would introduce additional data, event-study and publication
defects. Repairs retain current provider entitlements, risk constraints and
broker boundaries. No trades, subscription purchases or repository-visibility
changes are part of this work.

## Current evidence and access

Initial review base was `4592677ae458f03bc1dcc6afcfb05c941868ae36` (#730).
The independently checked workflow-only #731 merged to
`adc0fdca10747e42934f37edd6e84c3c4d3e26ec`, the repair branch's base.
The service's manually deployed revision remained `4592677` during inspection.
Times below use Asia/Riyadh; native evidence timestamps retain UTC where stated.

| Surface | Verified evidence | Limit |
| --- | --- | --- |
| GitHub | Main code, PRs, strict required checks, Actions logs, protected merge and authenticated CLI access | Repository-variable reads can return integration-specific 403; inspect resolved job behavior instead of assuming a setting |
| Render | Confirmed My Workspace; service `srv-d4hnir15pdvs739bqe1g`; deploy `dep-db3bb2l9fdbs73ajc50g` live at 00:13:14 on 8 Oct; both health endpoints ready with exact `4592677` and engine 5.151.3 | Auto-deploy is disabled; merging cannot establish a deployed release |
| Workbook | Targeted production ranges readable; 49 tabs, 40 hidden; active five-member portfolio and final board inspected | Connected tools do not install or execute bound Apps Script |
| Provider access | Actual transport contracts, source routing and trusted Actions refreshes available | No guessed credentials, purchased feeds or invented instrument mappings |
| Broker | Earlier positions/order readbacks available | Cash/balance APIs previously failed; this review cannot assert reconciled available cash |

The current Render instance used approximately 0.14% of its two CPU cores and
2.6% of 4 GiB memory in the sample. Neither this sample nor the retired instance
shows resource saturation. HTTP latency metrics were empty; no latency or
throughput benchmark is claimed. Sampled logs are kept private and are not
reproduced in public evidence.

## Reproduced defects and repair acceptance

| Priority / requirement | Reproduction | Repair and acceptance |
| --- | --- | --- |
| High: portfolio refresh / D9 | Successful scheduled run [37670931393](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/37670931393) resolved five source keys that omitted My_Portfolio | After validation, scheduled full runs append PF with order-preserving deduplication. Manual subsets, health-only and single-key runs retain their existing scope. Empty/invalid keys still fail. The documented JSON-array override now preserves quotes until parsing |
| High: portfolio units / D6, D10 | Active native ledger includes acquisition fees, while the backend injects bare buy price; later native updates change the same cost basis | Freeze raw buy price, native buy fees and fee-inclusive total together. Derive average cost once, reject ambiguous fee/currency headers, and prove restoration/price updates do not add fees again |
| High: checkpoint creation / units | All 61 sampled active 1W/2W checkpoints have same-entry same-day 1M witnesses, but their target prices treat fraction ROI as percentage points, producing a 100× scaling error | New targets derive both ROI in percentage points and price from the same explicit entry/1M forecast pair. Historical rows are immutable; diagnostic correction requires a unique same-entry witness |
| High: strict calibration / readiness | Target-unit ENFORCE can retain legacy PASS when corrected sample count is zero or measurement errors | Publish PENDING and unknown corrected metrics, retain legacy statistics only as diagnostics; never promote unavailable corrected evidence |
| High: margin cache contract / D6 | A witnessed thin-margin conversion loses its unit proof in fundamentals LKG/cache. Restored enforce publication can be 100× wrong | Value-bound per-field unit metadata follows actual merged fields through cache and restore. Enforce conversion is idempotent; legacy or stale unproved units remain unresolved. Off/observe numerical behavior is retained |
| High: forecast basis / D7, F7 | A settled negative ROI can become positive at publication while valuation, opportunity and overall scores retain the negative basis | Resolve existing forecast-pair policy then tuple enforcement before canonical scoring. Remaining late repairs withhold dependent scores and new investment actions; preserve existing exits and vetoes. Only clean newly acquired and scored factory evidence can release holdback |
| High: diagnostic privacy | Actual HTTP clients, application logging, provider errors, route envelopes and task serialization echo synthetic credentials | Shared dependency-free redaction at diagnostic output boundaries, including supported credential aliases/forms. Preserve raw provider classification internally, business values, HTTP statuses and retries. Test real mocked transports and mounted handlers |
| Medium: news ingestion / NEWS-1.6 | Summary helper calls an unsupported keyword; canonical source/time fields are ignored; substring matching accepts spoofed domains | Restore documented payload API, use canonical fields, parse hostname boundaries, distinguish aggregator transport from asserted publisher. Corrected publisher metadata does not assert primary verification |
| Medium: calendar continuity | Malformed prior date can erase carry; vanished symbol with only future ex-dividend is dropped; carried data gets a new timestamp | Validate fields independently, carry either future event, preserve original source/as-of or explicit unknown; retain existing schema/provider precedence |
| Medium: forecast evidence | Shortcut Spearman formula gives incorrect values and even the wrong sign with tied ranks; constant arrays report a numeric association | Correlate average tie ranks, validate finite/equal inputs and publish null plus reason for undefined results. No Brier/CV/model/promotion threshold changes |

PF currency safeguards default to enabled in all three production write lanes;
an explicit repository value `0` remains an opt-out. Direct CLI helper defaults
are unchanged. A merge or configuration expression alone does not prove the
resolved value in a production run.

Production scoring settlement, tuple coherence, margin publication and
historical target-unit measurement retain their observation settings. The
strict-mode repairs do not silently arm a new model or calibration policy.
New checkpoint creation and fee-inclusive portfolio arithmetic are corrected
independently of those settings. Fundamentals unit enforcement remains enabled.

## Coverage and decision evidence

The linked full audit [37680095784](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/37680095784)
finished its verdict at 23:12:22 on 7 Oct (20:12:22 UTC):

| Page | Distinct published members | Genuine usable acquisition | Full audit |
| --- | ---: | ---: | --- |
| Market Leaders | 255 | 99.22% | FAIL: below configured 1,025 minimum |
| Global Markets | 6,609 | 95.07% | FAIL: name coverage 97.93%, below 99% |
| Commodities / FX | 453 | 95.36% | PASS |
| Mutual Funds | 2,474 | 96.73% | FAIL: below 4,496 minimum and name coverage 98.02% |
| My Portfolio | 5 | 100% | PASS, but not refreshed by that scheduled source-key set |

The audit exits unsuccessfully for these coverage failures. A successful outcome
audit or sync process cannot supersede this verdict. Acquisition success also
does not prove current market quote age or executable eligibility.

The user confirmed that no repository, provider or Drive roster is approved
today. The live page Symbol column defines current membership, but it cannot
recover members already lost. The July 16 staged-expansion member list was
never committed. Older inspected backups mix different markets/scopes; they
cannot establish the intended current universe. A search for an early matching
backup is read-only. Any restoration requires an approved roster, versioned
files, backup and reviewed membership diff before workbook changes. Do not lower
coverage floors or manufacture members to turn an audit green.

The latest inspected board was WITHHELD with zero executable seats, zero
research/grace ticket amounts and masked financial KPIs. This supports the prior
funding repair; it does not establish current cash or investable opportunities.
The latest explicit native version witness remains 1.12.1. Complete 1.12.2 source
is merged, but installation/execution in the bound workbook is unverified.

## Remaining requirements and changed plan

| Area | Disposition after the bounded repairs |
| --- | --- |
| Data identity and coverage | Approved universe membership, unresolved issuer names, reviewed aliases/retirements and crypto asset IDs need primary witnesses. Withdrawn #732 proposal is not implemented: its alleged fundamentals-bearing unpriced provider shape is not emitted by the real adapter |
| Risk policy and cash | Existing position/sector/name limits remain. The PDF's 10%/40%/three program differs from live 20%/30%/two; broad authorization is insufficient evidence to loosen limits. Broker cash, settlements/reservations and order provenance remain unreconciled |
| F7/readiness | This release repairs forecast-basis consistency. Generic settlement errors/nonconvergence, session-calendar fallback and a shared decision bundle remain separate work; process health must not be called decision readiness |
| Horizons and precision | Populated 3M/365 horizon conflicts lack an authoritative contract. Asset-specific precision and fixed-income coupon/accrual contracts need dedicated source-backed acceptance |
| Calendar | Issuer-confirmed precedence, partial-provider event fills, provenance consumption, unreadable prior sheet and clear-before-write transaction safety remain. Sticky carry does not solve these requirements |
| Performance / F9–F15 | Corrected historical checkpoint errors still need same-cohort baselines and prospective, point-in-time out-of-sample evaluation. Short-term rank evidence does not establish probability calibration, profit probability or long-horizon skill. Existing full CV fold ordering remains a separate defect |
| News / NEWS-1.1–1.6 | Embedded prototype remains inactive: first-seen identity, target/acquirer roles, event direction, duplicated-event fitting, winter venue timing, provider error envelopes, schema/write safety and privacy need repair before observation activation. No unfitted event outputs enter sizing or new vetoes |
| Platform | Compatible audited dependency upgrades, dispatcher idempotency, safe Trade Notes and a durable event store/worker require separate contracts. Empty latency evidence does not justify new infrastructure |
| Open PRs | #720 remains unsuitable for whole-PR merge; #721's broad readiness claims need corrected journal acceptance. Extract only reviewed, production-shaped changes; conflict-free does not mean accepted |
| Native release | Bound Apps Script 1.12.2 must be installed and executed, then verified by versioned run-log and final board witnesses. Connected APIs cannot complete this step |

The next development sequence is: validated universe and identity inputs;
issuer calendar/write safety and reconciled cash/order evidence; shared
readiness/bundle transport; prospective forecast calibration and observation-only
event work. A measured source pilot precedes paid additions or queue infrastructure.

## Validation and release record

The parent release enrolls every new regression in blocking CI. Pure suites use
the actual lean Python 3.11 environment; transport and mounted-route suites use
the production-pinned contract stack. Existing tests and all four required
checks remain. Independent reviewers check frozen source bytes and actual
producer/consumer paths rather than only helper-level mocks.

All 14 checked-in non-publishing required workflow steps passed on the final
combined runtime, tests and workflow contents. The local documentation/commit
record is completed afterward; exact-head GitHub CI verifies the published SHA.

| Combined Python 3.11 check | Result |
| --- | --- |
| Blocking lean command after live-acceptance follow-up | 1,354 passed, 13 subtests passed, six explicit optional skips |
| Production-pinned schema/route/transport command | 146 passed, two existing schema skips |
| Full-source Apps Script funding | 64 passed |
| Fail-closed policy | 18 passed; deterministic artifact generated |
| Workflow audit | Seven tests passed; scanner zero errors, 27 existing warnings |
| Daily full-requirements command and sync harness | Passed; exact result recorded in the release receipt |
| Compilation and diff checks | Passed |

Independent reviews cleared frozen news/calendar/rank/schedule candidates,
the complete diagnostic-output repair, and fee/checkpoint/margin contracts.
The parent separately reviewed forecast-basis changes and ran 181 affected
tests before integrating units. The combined command then found and corrected
two test-boundary issues: inherited global logging disable, and an old untyped
margin fixture. The latter now proves both preserved value-bound units and
legacy unresolved units; neither correction weakens production safeguards.

Protected merge, exact Render revision and targeted live readbacks are recorded
in the release receipt. Code completion, deployment, scheduled execution and
native installation are distinct acceptance states.

## Live acceptance and bounded follow-up

[Repair #733](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/733)
protected-merged as `e4af2732d569347cea650652aba59a6af828aa80`. Its deployed
tree equals the reviewed head tree. Render deploy `dep-db3c7dfavr4c7395jntg`
went live at 01:13:42 Riyadh on 8 Oct (22:13:42 UTC on 7 Oct). Both health
endpoints report ready, engine 5.151.4, entry 8.14.2 and the exact revision.
Authenticated [readback 37694883495](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/37694883495)
passed signed research, zero blocked allocation and rejection of changed cash/FX.

The first portfolio-only [run 37694886900](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/37694886900)
exposed a blank native Buy Fees cell not represented in the initial fixtures.
The guard failed before fetch/clear/write and retained all five prior rows.
A unique same-snapshot native Cost Basis equals that row's Shares × Buy Price
exactly. Sync 6.64.10 resolves only this witnessed zero ledger fee component,
retaining the raw blank and proof; it does not assert a broker commission.
Missing, ambiguous, approximate, SAR-only and nonblank invalid fee evidence
remain fail-closed. Fifty-nine new regressions join blocking CI.

The run also exposed post-sync writes from an empty GM matrix leg. Ownership
receipts now suppress no-key and health-only dashboard writes; performance
tracking remains exactly one owning GM leg after a full sync. A single-page
refresh cannot launch unrelated performance recording. The matrix and tracking
policy, arguments and observation settings are unchanged. Sixteen added
workflow cases cover these boundaries. Independent combined review passed
281 affected tests, and all 14 required local steps were rerun successfully.

That existing performance writer separately created 232 new records, including
116 one-/two-week checkpoints at 01:17:36 Riyadh. All 116 have unique same-day
same-entry 1M witnesses and correct percentage-point ROI and four-decimal target
price. Historical target tuples were not rewritten. Portfolio write acceptance
is distinct and requires a successful 6.64.10 writer/readback.

The bounded Drive search found the earliest relevant accessible backup on
26 July: Market Leaders has 1,025 unique symbols, Mutual Funds 2,475. No
16–25 July backup or joint 1,025/4,496 witness was found. This file is evidence,
not an approved roster; no workbook membership changed. The latest native board
still withholds all ticket money but has an inconsistent cash-floor observation,
and bound 1.12.2 installation remains unverified.
