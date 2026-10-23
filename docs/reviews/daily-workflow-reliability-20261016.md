# Daily workflow reliability review — 16 October 2026

Reviewed current main `11ef13456e5b8775b74e677a94c3e383618b7d6b` in an isolated
worktree. This is a source review of all **36 versioned workflows**, including
**19 scheduled workflows**, their direct script entrypoints and inline runners,
plus **755 unique Actions runs** observed since 13 October. Historical weekly
runs were inspected separately where no run fell within that window. The
registry's 63 workflow IDs include historical definitions, not 63 current files.
An additional 326-run scheduled history window since 2 October covers every one
of the 19 current scheduled workflows. It overlaps the three-day sample; the
counts are not additive. The evidence summary records each latest schedule's
source SHA and outcome.

The hourly observation/repair task is enabled for 24 occurrences through
**17 October, 10:28 Riyadh**. It is a bounded hourly task, not uninterrupted
execution. This report records the initial repair pass; it does not claim the
24-hour observation has already completed or that every live workflow is healthy.
The durable starting checkpoint is
[`workflow-watch-20261016.json`](../evidence/workflow-watch-20261016.json).

## Priority and effect on the project

| Priority | Finding and evidence | Effect and current disposition |
| --- | --- | --- |
| P0 | All nine scheduled dashboard syncs observed in the three-day window failed. Latest source run [38821487562](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38821487562) published 6,609 Global Markets rows; recovery certified only 6,160 acquisitions, **93.2062%**, below 95%. All 6,609 retrieval timestamps were fresh, but 341 rows were failed/quarantined/preserved and 108 lacked a valid price. | Data availability/correctness and decision publication are the first repair priority. The audits are correctly refusing incomplete data. Do not weaken them. Recovery recorded 252 P/E quarantines; underlying source triples/vintages are absent, so a safe vendor-data correction is not yet established. Added count-only acquisition receipts to make subsequent diagnosis durable. |
| P0 | Standalone recovery lacked safeguards and operator-control mappings present in both daily writer lanes. Persistence sanity could default off; quota checks lacked the local EODHD key. | Fixed recovery parity for restored-row sanity, quota protection, identity/retirement/quarantine/currency and hold/status controls. Independent page selection and recovery budget remain unchanged. New opt-in controls keep existing defaults. |
| P0 | Adversarial replay testing found that a later recovery log with no terminal page verdict could leave an older successful attempt authoritative, in both full-fetch modes. | The follow-up requires scoped terminal evidence for the latest attempt. Missing/conflicting proof blocks the audit/planner; a page is never guessed from unrelated artifacts. Both coverage modes and the real recovery layouts are regression-tested. |
| P1 | The manual recovery planner downloaded primary logs but omitted the same source run's inline-recovery receipts. A primary failure could therefore trigger another fetch after successful inline recovery. | Added narrowly scoped optional recovery-artifact download before planning. Filesystem regressions prove successful recovery avoids replay, while a later failed/incomplete recovery remains blocked; older source runs without receipts still use primary evidence. This reduces duplicate provider work without certifying missing evidence. |
| P1 | The 11 October hypothesis run [38161414138](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38161414138) requested 730 days for 16 symbols, but every Yahoo request used `range=1mo`; all six hypotheses had zero events. | Fixed history argument adaptation: Yahoo receives `period=2y`, days-aware providers retain exact day counts. This restores the requested input depth and useful research coverage, not a promise of model accuracy or future trigger events. |
| P1 | Scorer publication exceptions and false registry-save acknowledgements could leave jobs green despite missing/partial output. These are source defects reproduced offline; recent successful job logs do not show that they occurred in those runs. | Fixed nonzero operational failures, truthful `WRITE_FAILED` receipts and acknowledgement checks. A correctly published S-1 policy FAIL still counts as successful execution. No scoring/promotion policy changed. |
| P1 | All three recent provider-target schedules rejected an empty cached reference originating from an older failed audit. Current cache-save guards are already correct. | This is an unresolved initial-reference prerequisite. Bootstrap only after an independently verified healthy full sync; do not accept a lower reference or bootstrap automatically to turn the alert green. Exact conditional operator procedure is documented. |
| P2 | Backup retention counted repeated Drive filenames as separate dates, then mapped each name to one ID and could repeatedly try deleting that same copy. Monthly deletions also distorted its daily count. | Fixed distinct-date retention, one attempt per expired file ID, preservation of retained duplicates/foreign files and accurate remaining-date counts. GCS object helpers and storage configuration are unchanged. No live backup was deleted by this review. |
| P2 | Only six of 63 intraday cron targets produced scheduled runs over 13–15 October; all six started after the declared market window despite green exits. | Scheduling delivery is a real service limitation, separate from script exceptions. Recorded exact target/start evidence. Do not treat a delayed green run as a 15-minute refresh SLA, backdate captures, or add provider load blindly. External dispatcher/cadence decisions remain open. |
| P3 | The workflow auditor reported optional empty-fallback Variable mappings as active force-refetch overrides. | Fixed static reporting. Literal nonempty overrides remain visible; expressions are not presented as live Variable readbacks. |

## Hourly follow-up: 16 October, 10:28 UTC / 13:28 Riyadh

New scheduled sync [38878037327](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38878037327)
is running unchanged main `11ef1345` with runner 6.69.0. Its primary Global
Markets leg acquired **6,217/6,609 rows (94.0687%)**, leaving 392 nonfresh rows
and needing 62 additional acquisitions to reach 95%. It restored 195 P/E
quarantines from last-good; no quota restriction or time-budget exhaustion was
observed. These are primary-leg counts, not a completed recovery result. Inline
recovery and final publication remain pending. Mutual Funds acquired 2,390/2,474
(96.6047%), Market Leaders 250/255 (98.0392%), and Commodities/FX 431/453
(95.1435%). Final name coverage is not established by the primary leg's capped
1,500-row validator.

Four new scheduled jobs completed: digest, calendar, shadow board and signal
backtest. Decoded logs confirm digest delivery with **NOT ACTIONABLE** status,
calendar publication, board execution and backtest artifacts. The board's risk
BREACH remains a domain outcome. Successful execution is not a live Sheet
readback or evidence that final sync publication passed.

The signal backtest run [38881044190](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38881044190)
exposed a concrete lookup defect: every pass skipped `Entry Risk Bucket`, while
the tracker writes the canonical `Risk Bucket` column. Backtester v1.1.2 uses
that canonical column for default all-signal analysis, falls back to the legacy
header only when the canonical header is absent, and preserves explicit signal
requests. Blank canonical values are never filled from legacy data. Statistical
calculations, evidence selection and policy thresholds are unchanged.

Calendar run [38882697051](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38882697051)
published successfully after an earnings-batch HTTP 404, also seen on 15 October.
The API path and parameter keys match the vendor contract. The job's 383 rows
include **355 retained prior-only rows** and 28 harvested rows, of which 27 have
at least one date; this does not certify fresh earnings coverage. Yahoo fallback
INFO messages are suppressed by the CLI's existing logging configuration, so
its successful field counts cannot be reconstructed from this log. Prior ASOF
and source preservation are unchanged; a maximum observation-age policy remains
an owner decision.

A separate request-format defect is verified: the calendar passed Yahoo's `.L`
London suffix through to EODHD, whose documented exchange suffix is `.LSE`.
Calendar provider v1.2.3 now canonicalizes that suffix only at its existing API
request boundary, preserving original result keys and existing deduplication.
The quote provider already implements this conversion. This adds no retry or
batch splitting and leaves Saudi/other-market behavior unchanged. The live 404
cause remains unproved until a deployed run supplies evidence; this repair is
not a claim that the supplier outage is resolved. Contract references:
[EODHD calendar](https://eodhd.com/financial-apis/calendar-upcoming-earnings-ipos-and-splits)
[EODHD exchange-code catalogue](https://eodhd.com/financial-apis/exchanges-api-list-of-tickers-and-trading-hours).

PRs #786, #787 and stacked #790 remain open and overlap the scorer/runner/version
pins in #785. Preserve both sets of fixes during any later integration; none is
assumed deployed. The backend remained healthy/ready at 10:28 UTC with deployed
SHA matching main and zero route failures/startup warnings/readiness reasons.
Follow-up validation passed 38 backtest tests and 122 calendar/publication/
redaction tests, including 29 new regression cases. Calendar checks ran in the
actual contract-only dependency environment. Independent review confirmed that
cohort/statistical functions and all existing calendar functions except symbol
splitting remain unchanged. Another 25 workflow/verdict/manifest checks plus
nine subtests passed; compile, fatal Ruff and strict workflow audit passed.
Verifier v1.0.64 matches 34 module and 34 script pins; its existing local
unarmed-switch DRIFT is not a production readback. Final-head CI results are
recorded in PR #785 after the follow-up commit is pushed.

## What was verified and what remains unknown

The backend was healthy and ready at 07:28 UTC / 10:28 Riyadh, with Python
3.11.9, application version 8.14.5, engine 5.154.0 and deployed commit matching
current main. An older deployed-readback failure correctly rejected a transient
commit mismatch. No build, launcher or production dependency change was needed;
Procfile comments were corrected while process commands stayed identical.

Recent tracker, shadow-board/scorer, scorecard, brief, policy and backup job logs
contain actual execution acknowledgements. Zero writes in observe mode, a risk
breach, NO_ACTION or insufficient S-1 evidence are domain outcomes, not proof of
a crashed workflow. Conversely, job success alone does not certify fresh rows,
atomic publication, prediction quality or delivery within its scheduled window.

Manual destructive repair/apply jobs and email jobs were not replayed just to
obtain green checkmarks. The review made no live Sheet, provider policy, broker,
Render setting, GitHub Variable or investment/trading-policy change. Code changes
are on the review branch and require publication before scheduled main runs can
exercise them. The separate old PR #750 remains open and is not assumed deployed.

## Validation and continuation

All seven new regression modules run in the genuine lean environment with only
NumPy, pytest and PyYAML. The combined suite, including existing workflow audit,
CI verdict, mapping and manifest checks, passed **164 tests plus nine subtests**
with no skips. A separate recovery integration run passed **105 tests plus nine
subtests**. Independent review also passed seven adversarial attempt-order and
artifact-boundary probes. Failure injection exercises actual script entrypoints
and the real Yahoo wrapper through a fake HTTP boundary; storage tests cover
Drive, OAuth Drive and GCS without remote writes. The backup self-test passed 7/7.

Before the final recovery-evidence repair, the complete local suite passed
**5,180 tests plus 252 subtests**, with 33 skips. Remote full CI at the first
published repair commit `45ceb27f` independently matched those counts; its modern
framework lane passed 16 tests with two skips. At recovery-evidence head
`523cc4f6`, exact-head remote full CI passed **5,238 tests plus 252 subtests**,
with 33 skips; the modern lane passed 16 tests with two skips. All blocking
checks passed. The hourly follow-up above changes that head again; its exact
latest-head outcomes are recorded in the PR rather than treating older green
checks as proof for a later revision. Four stale version expectations discovered
during the first full pass were corrected with behavioral assertions retained.

All 15 active Node harnesses passed, including Apps Script syntax. Python
compilation, fatal-error Ruff checks and the Render environment contract passed.
Local verifier v1.0.63 matches all 34 module and 33 script version pins. Its
strict local verdict remains DRIFT because the existing timeout/offloop guards
are unarmed in this shell. This is not a production flag readback; no controls
were armed to manufacture a CLEAN verdict. The workflow audit reports **zero
errors and zero warnings**; this static result is not a live-health verdict.

A read-only dependency review dispatched on the repair branch passed its typed
baseline and reported eight existing advisories across Starlette and nltk;
httpcore's direct-URL distribution was not audited. These remain disclosed
follow-up work, not a claim that dependency security is clear. No dependency pins
were changed in this repair.

During the observation window, re-read source SHAs and actual run artifacts,
compare new outcomes with the checkpoint, and repair only evidenced defects.
Recheck a failed correction rather than repeating an expensive writer blindly.
Keep 95% acquisition and 99% name coverage, identity and funding gates intact.
The final report must distinguish healthy, fixed awaiting publication,
provider/configuration blocked, intentionally skipped, unrun and unverified.

Supporting evidence:

- [Successful-run integrity follow-up and seven operational repairs](successful-run-alignment-20261017.md)
- [Complete workflow/entrypoint inventory](workflow-entrypoint-inventory-20261016.md)
- [Scheduler and concurrency review](scheduler-concurrency-review-20261016.md)
- [Learning workflow source/log evidence](learning-workflow-reliability-20261016.md)
- [Deployment and provider-control evidence](deployment-provider-controls-20261016.md)
- [Observed run summary](../evidence/workflow-run-summary-20261016.json)
