# Stack #804 integration — 24 October 2026

The owner authorized resolving stack conflicts and preparing the existing PRs,
without merging or deploying. The stack contains exactly #785, #791 and #792;
the separate draft #795 is outside this repair. Current main was verified at
`d4e8c643901570cd2b97d806a3229e998a721222`, including 23 commits after the
original `11ef1345` baseline. Earlier review documents describe their dated
snapshots; they are not current production-readiness measurements.

## History and preservation

Each existing branch advances through an integration commit retaining both
parents. No rebase, force push, PR merge, branch retarget or main push is needed.

| PR | Original head | Integration parent |
| --- | --- | --- |
| #785 | `2981d478` | current main `d4e8c643` |
| #791 | `afbfc13b` | updated #785 `7dcd9307` |
| #792 | `4c9007d8` | updated #791 `adbee766` |

The root integration retains XLSX validation before storage or retention and
the later resumable Drive/GCS upload recovery. Six combined regressions exercise
invalid exports, successful resumed uploads and exhausted transfers on both
backends. News archive schema/read-shape guards coexist with main's verified
identity, acquisition evidence, created-only initialization and non-retried
append. Missing requested-symbol evidence cannot be accepted from successful
batch metadata. Historical fixtures now supply the explicit acquisitions that
main already requires.

The sync retains Hong Kong reporting-currency checks, last-good read retry and
status-stamp retry alongside count-only acquisition diagnostics. Tracker
acknowledgements and explicit history windows coexist with direction-aware
metrics and the default-off witnessed target-close path. Neither retry budgets
nor historical maturity policy changes in this integration.

Native Portfolio retains main's inactive/closed/sold lot exclusion and the
pending response, cost-evidence, panel-preservation and completion guards.
Top-10 retains bulk rendering, subunit FX, terminal failure handling and typed
completion. The outer failure wrapper forwards options and returns the inner
completion result. Historical source hashes and the Portfolio panel golden
remain unchanged; exact inverse edits keep the old-source comparison meaningful.

## Explicit quantity conflict resolution

Original #791 allowed a full fractional EXIT suggestion. Newer main #816
requires whole-share suggestions. The integration preserves **main's current
whole-share contract**: floor custody, price using the true custody quantity,
and retain the unsold fractional value in post-trade weight. A sub-one-share
position has no sized sale. Proceeds available to funding are bounded by the
actual sized whole-share sale, so an unsold fraction cannot fund an ADD.

The superseded fractional-EXIT expectations are replaced with explicit current
contract, residual-value and proceeds assertions; main's original weight tests
remain intact. Five additional funding cases and four precision/conviction/
cash-reserve combinations cover this interaction. Public settled-new-cash
funding policy and all action, eligibility and quality thresholds are preserved.

## Combined versions and checks

| Component | Combined version |
| --- | --- |
| backup_workbook | 1.5.3 |
| news_archive_collector | 1.3.2 |
| run_dashboard_sync | 6.70.3 |
| track_performance | 6.47.1 |
| Portfolio native | 1.5.18 |
| Top-10 native | 1.13.14 |
| Decision Chain native | 1.0.3 |
| Opportunity Builder | 1.27.3 |
| Portfolio Actions | 1.16.3 |

CI retains every test path from original #785/#791/#792 and current main.
The combined backup boundary suite is added to the existing blocking lean lane.
The main news artifact upload is aligned with the repository's supported action
major. Existing required verdicts, dependency pins and runtime defaults remain.

The updated root tree passed 6,048 tests, 41 skips and 283 subtests locally.
Focused Python and full-source native batteries validate the dependent layers;
their counts overlap and must not be added. Compilation, fatal Ruff, manifest
pins and strict workflow audit are checked on the combined trees. Final
exact-head GitHub results belong in each PR body; older green checks are not
validation of these integration commits.

## Publication boundary

No live Sheet, installed Apps Script, Script Property, runtime flag, schedule,
secret or access setting is edited. No source/email writer is replayed, no
provider baseline is bootstrapped and no trade is placed. No PR is merged and no
deployment is requested. Source code and synthetic receipts do not certify the
installed native call graph, fresh account reconciliation or live publication.
All existing fail-closed gates continue to apply after any separately authorized
publication.
