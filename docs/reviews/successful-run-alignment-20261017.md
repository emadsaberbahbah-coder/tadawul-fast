# Successful-run integrity review — 17 October 2026

This follows the [daily workflow review](daily-workflow-reliability-20261016.md)
and the user's request to review scripts that still run successfully. A green
Actions conclusion must accurately describe execution; it does not by itself
certify fresh inputs, complete acquisitions, delivery, or restored backups.

The starting repair head is `ea66084c832d865772bd506e9d0434ff65002026` on PR
[#785](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/785). Production
still runs main `11ef13456e5b8775b74e677a94c3e383618b7d6b`. The seven defects
below were reproduced using synthetic inputs at the real script I/O boundaries.
They are verified source defects, **not claims that each occurred in a recent
successful production run**. No production writer, email, or destructive backup
operation was replayed for this review.

## Repairs, ordered by effect

| Priority / script | Reproduced misleading success | Revised behavior and acceptance |
| --- | --- | --- |
| P0 — `run_daily_brief.py` 1.18.2 | A failed required `_Status` read became an empty page, discarding the feed veto; the real CLI could send an executable brief and return zero. | An actual receipt-read exception returns 2 before model, rendering, PDF or SMTP work. A successfully read blank legacy receipt retains the existing behavior. Optional-page failures and financial calculations are unchanged. |
| P0 — `run_shadow_board.py` 1.5.1 | A failed `Portfolio_Decision` lookup/read became an empty holdings book; the CLI published `NO_ACTION` and an OK receipt without evaluating holdings. | An actual exception returns 2 before authority/provider calls or board/history/OK publication. Verified empty books still produce the normal no-action result; freeze and dry-run semantics are preserved. |
| P1 — `intraday_quote_refresh.py` 1.0.4 | Failed quote calls or failed target reads could produce empty plans, an OK receipt and exit zero. | Transport, response-envelope and required target I/O errors produce ERROR/PARTIAL receipts and exit 2. Independently valid page updates may still complete and are not retried. Legitimate empty scans, unchanged quotes and optional absent sources remain valid outcomes. Quote evidence and eligibility rules are unchanged. |
| P1 — `backup_workbook.py` 1.5.2 | A sufficiently large HTML/error body or corrupt XLSX passed the size check and could be uploaded before retention removed older backups. | Bounded stdlib ZIP/XML validation checks CRCs, OOXML relationships, worksheet structure and required core tabs before local backup copy or destination mutation. Invalid exports cannot upload or prune backups. Existing retention, Drive/OAuth/GCS paths and storage configuration remain intact. |
| P1 — `pit_snapshot.py` 1.1.2 | A failed legacy-header migration still appended 24-column rows beneath a 16-column header and returned zero. | Exact supported headers are recognized; new/migrated headers must be written and read back before append. Unknown or reordered populated schemas stop. Header/append failures return 3 with a stage receipt. Historical legacy rows are not rewritten; date, source-missing and duplicate semantics are unchanged. |
| P1 — `news_archive_collector.py` 1.1.1 | Failed historical reads became an empty duplicate set; a header failure/mismatch could relabel existing rows. A failed empty fetch could appear already complete. | Unreadable or incompatible archive state stops before append/header replacement. Initialization requires a proven empty worksheet. Existing-day checks require the canonical header. Legitimate duplicates and successful zero-article observations remain accepted; failed/unknown empty fetches return nonzero. |
| P2 — `send_digest.py` 2.1.3 | SMTP's partial-refusal dictionary was ignored, so one accepted and one refused recipient still logged that both were sent. | Both SMTP transports validate the acknowledgement. Partial refusal or an unconfirmed acknowledgement returns 2 with counts only; accepted recipients are not automatically resent. Empty refusal dictionaries preserve the successful path. Preview never connects. |

All new failure diagnostics above avoid logging private exception payloads.
There are no added provider retries, relaxed gates, new dependencies, credential
changes, or investment-policy changes. Version verifier 1.0.65 pins all seven
revisions and adds the previously unregistered news collector. CI 1.1.24 runs
the new failure regressions in blocking lean/heavy lanes; existing IQR contracts
already run in the lean lane.

## Verification

The six initial repairs passed **252 tests plus nine subtests** in the actual
minimal CI environment. A further **179 related tests** passed with pinned
production dependencies, including existing blocked-input, PIT repair, quote
epoch, learning-guard and openpyxl compatibility checks. Shadow Board adds 13
failure/normal/freeze tests in both environments; the existing freeze battery
passes all 31 checks. Stale freeze-harness version literals now use the loaded
script version; behavioral assertions remain intact. Workflow/manifest/verdict
checks and the strict static workflow audit remain required.

The new regressions exercise actual entrypoints against fake Sheets, storage,
provider and SMTP boundaries, including normal and deliberately failing paths.
They do not certify live Sheet readback, SMTP inbox delivery, restored financial
correctness, or atomic deduplication between concurrent writers. Backup fixtures
include real workbooks generated and reopened by the pinned openpyxl reader;
there was no live restore. Required checks for the final pushed SHA are recorded
on PR #785; older green checks are not reused as validation of this revision.

## Latest production checkpoint — 17 October, 02:58 Riyadh

Scheduled sync
[#38978392280](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38978392280)
completed FAILED at 02:58:50 Riyadh on unchanged main. Required acquisition
floors pass: GM 6,280/6,609 (**95.0219%**), MF 2,388/2,474 (**96.5238%**),
Market Leaders 253/255, Commodities/FX 432/453. The
[Sync Outcome Audit](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38984181303)
passed. This does not establish a healthy full sync: GM names **98.0481%** and
MF names **98.9895%** still miss 99%. Decision checks retain stale PF/T10 source
comparisons and unproved pool reconciliation/full-universe claims; strict final
publication also lacks PF/T10 source-consumption proof. The 2,469 versus 2,474
pool aggregate does not prove that five rows were lost.

Final publisher receipts confirm `blocker_published=true`, `feed_promoted=false`
and audit exits `[2,2]`. These are intended quality blocks. No crash or distinct
new runtime defect is established by those failures. The latest GM name result
is lower than the preceding completed run's 98.1389%; aggregate counts alone
cannot identify an overwrite or its cause. Provider baseline bootstrap remains
blocked until an independently healthy full sync exists.

Other remaining limits are calendar earnings acquisition after the recurring
HTTP 404, the schedule-delivery SLA, independent output readbacks, and native
decision reconciliation/consumption receipts. A successful non-strict acceptance
measurement or observation-mode repeat count must not be turned into a new
enforcement policy just to change its color. Digest manifest run coherence also
needs a separate contract/policy review; this patch does not impose a new gate.

## Publication and observation

Changes are reviewable on PR #785 and remain **unmerged and undeployed**. Existing
overlapping scorer/runner/pin work in #786, #787 and #790 must be preserved during
later integration; #788's default-off orchestration is not deployed either.
No overall project-improvement percentage, speedup, cost saving, investment
return or model-accuracy gain follows from these operational regressions.

The bounded hourly watch ends **17 October, 10:28:42 Riyadh**. It is not continuous
execution or a guarantee that all scripts will pass. Publication requires the
user's explicit authorization; no such authorization is recorded.
