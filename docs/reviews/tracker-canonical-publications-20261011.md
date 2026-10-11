# Tracker canonical targets and publication replay — 11 October 2026

Tracker 6.45.0 integrates the upstream 6.44.0 same-day-repeat measurement
with PR 750's pure performance-math extraction. It preserves
`TRACK_DEDUP_SIGNAL_HISTORY=enforce` as the explicitly labelled measure-only
WP-7 contract. No rollout flag is activated by this delivery.

## Canonical targets

`TRACK_CANONICAL_TARGETS=1` creates prospective 1W/2W/1M/3M/1Y target tuples
in percentage points from the explicit positive finite entry/forecast price
pair. It does not infer units from ROI magnitude or fabricate a forecast price
from an unlabelled ROI. Missing forecast evidence remains missing.

Each new record carries a JSON receipt in its existing Notes column:
version 2, horizon, `roi_unit=percent_points`, forecast-price/missing-forecast
basis, and producer version. Keeping the receipt in Notes retains the
32-column Performance_Log geometry and the AG-side summary panel. Existing
stored target cells are not migrated or rewritten by this feature.

The read-only `canonical_performance_report` audits every horizon. It separates
positive/negative price-movement WIN/LOSS from at-horizon target achievement
and forecast error. Valid explicit flat forecasts contribute to forecast-error
statistics but have no directional target-achievement denominator. Multiple
matured copies of one cohort key are excluded. Non-numeric, blank or nonfinite
stored realized returns cannot enter the new audit as certified zero returns;
legitimate numeric strings remain accepted.

Legacy price/ROI agreement is insufficient provenance because older writers
could synthesize the target price from the raw ROI. The audit exposes a
`price_consistent_candidate_pp` and `candidate_unit`, while leaving the
canonical value unresolved. Checkpoints require an explicit, corroborated,
unique same-day 1M witness before a conversion can be accepted. Tiny targets
consistent with both units remain ambiguous. No historical source-price
registry is available in this checkout, so those legacy records require
independent evidence before migration.

The active rollout logs `[CANONICAL-PERFORMANCE]` and adds the schema state to
_Run_Log Details. With `--export`, a separate `_canonical_performance.json`
file is written for every output format. The ordinary legacy win-rate and S-1
calibration publication retain their previous definitions and rollout controls.
This new audit does not certify legacy accuracy or replace the S-1 sentry.

## Immutable signal publications

`TRACK_SIGNAL_PUBLICATION_MODE=immutable` uses an explicit producer
`publication_id`/`source_run_id`, or `TRACK_PUBLICATION_ID` supplied by the
workflow. Both writer lanes map `TRACK_PUBLICATION_ID` to `github.run_id`,
which survives a workflow rerun. Publication IDs must be globally unique
across all producers sharing Signal_History; the key binds the symbol and publication ID,
never the capture clock. Replaying after Riyadh midnight therefore retains
the same identity.

Signal_History end-appends Publication Key, Publication ID and Publication
Schema. The legacy symbol/day Key and its column positions remain unchanged.
The immutable writer re-reads the open-ended publication-key column inside
each retry. A committed append whose response was lost is observed before a
second append, even if the worksheet grew past cached row-count metadata.
Failed/malformed identity reads and missing identity refuse the opt-in write
and register an evidence-append failure. A later source publication receives
a distinct row, so the first publication is retained alongside subsequent
states. Distinct same-symbol publication IDs in one batch are retained.

The read-only daily projection and `_signal_publications.json` export expose
first publication and latest observed daily state separately. Latest observed
is not a certified final state: capture completeness/session-close evidence
is still required. Existing intraday-flip consumers continue receiving every
distinct publication. Existing same-day duplicate rows are not compacted.

## Rollout, validation and limits

Both tracker lanes map `TRACK_CANONICAL_TARGETS` with fallback `0` and
`TRACK_SIGNAL_PUBLICATION_MODE` with fallback `off`. Rollback sets 0/off;
previously recorded identities and schema receipts remain available.

The focused canonical/publication and mapping suites passed 46 tests. The
existing target-unit, zero-baseline, matured/signal-dedup, forced-coverage and
math-extraction suites passed 28 tests. Added real writer-path checks cover
fresh-process replay, midnight replay, distinct publications, failed and
malformed index reads, lost append responses, stale grid metadata, schema
round-trip, zero forecasts, numeric outcome strings and legacy ambiguity.

Google Sheets does not provide an atomic unique constraint. Independent
simultaneous writers can still race between index read and append; the
existing separate workflow concurrency groups and best-effort overlap check
do not provide global serialization. A simultaneous same-publication race
requires a shared writer service or durable lease before claiming strict
cross-writer idempotence. No production idempotence claim or activation is
made here.

At-horizon achievement is not an intrahorizon target touch. Price-path
observations are still needed for true target/stop hits, and broker fills,
fees, FX and costs are needed for net realized trade results. Those fields
remain explicitly unknown. Immutable publication identities are not broker
fill lineage, and existing Performance_Log cohort keys still use the capture
Riyadh day. Historical compaction, calibration gate activation and a fixed
out-of-sample cohort remain separate work.
