# Coherent dashboard refresh candidate — 3 November 2026

Base: `3bbf3c80da8e543d217b122d8d8c3dcf0a1c3c8b`.

The dashboard reads nightly BigQuery copies. Refreshing Looker queries those
copies; it does not acquire new provider data or copy a newer workbook. This
candidate adds an opt-in archive refresh after a completed production sync and
keeps the latest refresh diagnostic separately visible. It does not change
scoring, economic gates, orders, executable counts, or report styling.

## Source capture and decision permission are separate

`audit_capture` runs the actual coverage and freshness **source contracts** on
exact frozen inputs. `dashboard_admission` verifies source cohort, headers,
complete-column bounds, physical membership, pool reconciliation, clocks and
portfolio consumed/published row parity. Neither `release.accepted` nor the
final-release writer is called. The legacy auditor's `decision.executable` flag
is not consulted.

A coherent native route with `status: ok | output: WITHHELD | Selected=0 exec`
and NO_NEW_ENTRY or learning-pending reason is copied unchanged. Its reasons,
zero tickets and actual timestamps remain visible. Capture never grants trading
permission or changes the decision feed. Native B2 `status: WITHHELD` currently
means **source blocked**, not an economic output; source-readiness WITHHELD or
mismatched consumed rows cannot be overridden by zero executable tickets.

The explicit account-count check compares the native full `holdings N` count
with published My_Portfolio rows. It does not compare broker execution scope or
exclude cash/income rows. Thus consumed six versus two published remains blocked
even with recent timestamps or a forged VERIFIED run marker.

## Publication and history

- `TFB_BQ_AFTER_SYNC` defaults off. The original nightly schedule/manual dry run
  and untouched legacy script remain the off path. `TFB_BQ_ARCHIVE=off` is the
  master kill switch. While armed, all archive writes use one coherent path;
  partial manual tab selections are refused.
- Workflow admission is limited to completed main-repository production
  schedule/manual syncs. Source provenance is checked independently. The wait
  for both matching consumed-source receipts and OK route states is bounded to
  45 minutes. One prewrite recapture is allowed after a content change; writer,
  provenance, audit and mutation faults are not blindly retried.
- Capture 17 archive tabs plus all source-audit inputs. Audit exactly the
  archive's existing unformatted-value/formatted-date representation. If a date
  format hides intraday precision, the existing source rule withholds it; no
  later serial read is borrowed to green an older payload. Recheck critical
  content and the production writer state before publication.
- News, calendar, cost-basis and learning inputs have independent writers. Their
  own capture times are retained. They are not falsely attributed to the sync.
  All frozen inputs remain in private BigQuery history, not a public artifact.
- Content identity binds source run/attempt/SHA, capturing code/version, policy
  and captured content. Reused stages retain their original times. Validate
  exact table identity, schema/modes and DAY partitioning; incompatible existing
  tables are never relabeled.
- One BigQuery transaction updates all 17 current-day partitions and inserts
  immutable history and its receipt. Prior days remain untouched; earlier
  same-day captures remain in history. Source/attempt/time ordering prevents a
  delayed old capture from replacing newer data.
- Mutation job IDs are stable. Lost submission/upload responses are resolved by
  authoritative job identity/configuration readback, not blind replay. Unknown
  outcomes remain UNKNOWN. Current-generation readback is a fresh SELECT.

## Latest diagnostic without replacing coherent data

Every terminal write attempt independently appends metadata-only
`dashboard_refresh_attempts`, including source ID, state, reason codes, consumed
and published counts, and its own time. A source failure never promotes the raw
source tables. The latest diagnostic therefore exposes the new failure while
charts retain their actual last coherent data/time.

`docs/evidence/dashboard_refresh_status_query.sql` is a ready Looker custom query
joining the latest attempt and last coherent receipt. It also handles the first
diagnostic append failing after data committed, no receipt yet, and an old
attempt completing after a newer source. The two metadata tables are initialized
before data publication; no new permissions or sharing are requested. Add the
source/tile only after those tables exist and their schemas read back correctly.

The receipt column `committed_at` stores BigQuery's **transaction-start**
CURRENT_TIMESTAMP, not exact job completion. The query labels it publication
transaction time, checks source ordering separately, and exposes data/diagnostic
ages. It never treats a copied-data timestamp as quote freshness. A diagnostic
storage/auth outage is explicitly UNKNOWN in the Action artifact; it is not
reported as successfully delivered to Looker.

## Validation and adoption

Offline tests include the real legacy partial-publication defect, full current
source cardinalities, actual frozen source audits, coherent economic withholding,
forged and genuine account/source mismatches, sparse tails, schema/transport
failures, atomic transaction scope, old-capture refusal, committed-then-timeout
recovery, conflicting diagnostic retries, both kill switches, and a workflow
admission truth table. Final combined relevant run: **296 passed and 10 subtests
passed in 142.41 seconds**. Independent review separately passed the nine real
source-audit cases, four diagnostic cases and nine staging cases and found no
unresolved offline blocker. These are overlapping verification runs, not extra
independent investment samples. Ruff E9/F63/F7/F82, compileall, diff checks and
repository workflow audit (0 errors, 0 warnings) pass.
Local Python is 3.12.14; CI's Python 3.11 and live BigQuery remain separate checks.

### Isolated staging with existing Action credentials

No repository-wide activation is required for staging. Use the existing
**BigQuery Archive** Action, selecting this reviewed branch. The existing
GOOGLE_SHEETS_CREDENTIALS secret is consumed inside the Action; do not copy,
print, rotate or broaden it. The target project remains the configured project.

1. Confirm a private, US-location dataset named, for example,
   `tfb_archive_staging_transport_20261103` exists under the existing project IAM.
   The code does not create datasets or change permissions.
2. Dispatch with `write=true`, that `staging_dataset`, `staging_checks=true`,
   and blank `tabs`. This runs synthetic publication tests only: real atomic
   replay, a DELETE followed by the expected ASSERT rollback, rejection of an
   older source, all 17 table signatures unchanged after negatives, and actual
   diagnostic/status query readback. Its artifact explicitly says real source
   policy capture is still PENDING. Never connect the production report to
   synthetic fixture tables.
3. Use a separate private `tfb_archive_staging_source_20261103` dataset for the
   real policy cycle. Dispatch `write=true`, `staging_checks=false`, blank tabs,
   and the exact completed `source_run_id`. A real coherent business-withheld
   cycle must copy unchanged with zero tickets. A mismatched/stale source must
   retain existing coherent data and expose a dated diagnostic instead.
4. Read the Action artifacts and BigQuery receipts back. A positive fixture,
   green workflow or successful load alone does not satisfy source acceptance.
   Verify actual schemas, source identities, history, data totals and the
   status custom query, including its no-diagnostic and refused-source states.
5. Only after review, CI and live acceptance, merge the exact tested commit,
   enable `TFB_BQ_AFTER_SYNC=on` through the authorized configuration path, and
   observe one full production source/capture cycle. Check the running source,
   mutation job results, receipt, diagnostic and Looker refresh before adoption.

Both staging inputs default off/blank. The target must match
`tfb_archive_staging_[A-Za-z0-9_]{1,128}`; synthetic checks without that isolated
target are refused before authentication. The master archive kill switch still
applies. No staging run was dispatched during authoring.

### Looker refresh-status source availability

Add the supplied custom query only after both metadata tables exist with these
schemas, using the existing report owner's BigQuery connection and current IAM:

| Table | Required fields |
| --- | --- |
| `dashboard_refresh_attempts` | attempt_id STRING, snapshot_date DATE, attempted_at TIMESTAMP, nullable source_run_id/capture_id STRING, state/reason/diagnostic_json STRING |
| `dashboard_snapshot_receipts` | capture_id/source_run_id STRING, source_attempt INTEGER, captured_at TIMESTAMP, snapshot_date DATE, receipt_json STRING, committed_at TIMESTAMP (transaction-start time) |

No new sharing or permission grant is part of the change. The query always
returns a status row when tables exist, even before any diagnostic or coherent
receipt. Show source/diagnostic times and age with the status, plus the observed
business withholding/zero-ticket state; do not substitute the latest attempt
clock for quote timestamps. If a table is unavailable, show status unavailable,
never a default green/current label.

No live BigQuery write or workflow dispatch has been performed during authoring.
This is a default-off candidate, not production adoption. Before activation:
verify the exact project/dataset, existing Action credentials/IAM, absence of an
old archive writer, isolated live transaction rollback and readback, a real
coherent source capture (including an economic withholding case), a refused
source/account case with current diagnostics, then the Looker cache refresh and
visible source/diagnostic times. Keep real unavailable/stale states visible.

The Sheets reads are not a provider transaction. Frozen audits, content rereads
and writer probes establish the observed stable capture; no global cross-service
lock or exactly-once acquisition is claimed.

Rollback: unset `TFB_BQ_AFTER_SYNC` for the original path; set
`TFB_BQ_ARCHIVE=off` to stop archive writes. Retain history/receipts. Do not delete
newer unrelated work or falsify clocks.

References: [BigQuery transactions](https://docs.cloud.google.com/bigquery/docs/transactions)
and [atomic job completion](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job).
