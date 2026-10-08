# Witnessed performance outcomes — 9 October 2026 (Asia/Riyadh)

`track_performance` v6.43.0 prevents a positive scalar, preserved row, stale
quote, or delayed current quote from manufacturing a historical WIN/LOSS.
This is a prospective outcome-data repair. It does not establish forecast
accuracy, a probability of profit, or permission to activate a model.

## Outcome contract

The actual backend price chain and Yahoo chart fallback return an ordinary
numeric-dictionary interface with an `OutcomePrices` sidecar. Each immutable
receipt binds one symbol, exact price, provider, actual acquisition time,
source quote time and timestamp kind. The consumer revalidates both receipt
and numeric value. Casting to a plain dictionary, changing its numeric value,
or substituting an old scalar-only fallback loses evidence and holds the
record pending.

The shared acquisition predicate remains authoritative for failed, preserved,
quarantined, unverified and explicitly stale provenance, alias conflicts and
provider capabilities. Outcomes additionally require an explicit successful
acquisition receipt, a bounded acquisition age (24 hours), and a precise
source quote timestamp. A generated `Last Updated` value or generic
`timestamp` cannot substitute for either receipt or quote time. Conflicting
duplicates inside one HTTP envelope fail closed; a separately successful
fallback can provide independent evidence.

The existing offline `exchange_calendars` dependency identifies the first
exchange-session **close at or after the original target instant**, including
holidays, weekends, daylight saving time and early closes. Its target session
must have completed, and the witnessed price must belong to that session
at its close. Quotes before the close, even one second earlier, remain
pending. An explicit nonempty and validated Yahoo `regularMarketTime` can
explain a regular-market timestamp at most 15 minutes after close; the generic
future-clock allowance cannot certify an after-hours price. Unknown calendars
or missing calendar dependencies produce an explicit unknown reason.

The price must also belong to the latest completed or current exchange
session when fetched. A current quote from a later session can update a
current-price fact, but cannot relabel a delayed 1W/2W/1M record as if it were
the intended target close. Corporate-action verification, budget deferral
and adjusted ROI handling remain in place after provenance checks. The actual
audit ends adjusted history at the exact target-session bar and includes only
confirmed actions effective on or before that session. Missing, conflicting
or invalid target closing bars remain pending. Legacy two-argument helper
callers retain their previous API. Infinite or undefined ROI cannot become
WIN or LOSS.

## Storage and diagnostics

The 32-column `Performance_Log` schema is unchanged. A single replaceable
`[OUTCOME-WITNESS v1]` JSON receipt in `Notes` records the schema/tracker version,
evaluation time, original target, provider, acquisition time, source time,
target calendar/session/close, requested horizon, intended elapsed days and
actual elapsed days. Successful outcomes also disclose the ROI basis and
realized return. Pending receipts give the skipped reason. Existing notes
are preserved, and repeated audits do not append unlimited receipts.

The run-verdict JSON exposes a reason-count histogram and evidence version.
Only ACTIVE records are evaluated. Historical MATURED/EXPIRED records are
unchanged; existing calibration/backtest consumers still contain that legacy
cohort, so their aggregate results must not be described as entirely
witnessed. The legacy unpriced-grace switch still permits an actually absent
price to become EXPIRED/UNPRICED after grace; it cannot disable mandatory
outcome evidence. Known failed, stale, ambiguous or wrong-session prices stay
ACTIVE with no realized return or outcome, including beyond that grace.

## Validation and follow-up

`tests/test_outcome_price_evidence.py` drives the real backend fetch chain,
actual async/sync Yahoo HTTP methods, fallback integration, maturation,
corporate-action seams and existing sheet serialization. Transport is mocked;
no provider or workbook is contacted. Tests cover healthy positive/negative/
breakeven outcomes, failed and preserved sources, aliases/duplicates, value
tampering, unknown timestamps/calendars, just-before-close and late quotes,
overflow, immutable history, bounded Notes receipts and schema round trips.
Full-dependency runs also exercise real exchange calendars for NYSE daylight
saving time, Thanksgiving, early close and the Saudi weekend. In lean CI only
that dependency-backed calendar test may skip; deterministic production-path
fixtures and missing-calendar fail-closed tests still run.

`scripts/test_maturation.py` uses fixed synthetic receipt-bearing prices and
an offline clock; fallback and corporate-action network paths are disabled.

This conservative contract can defer thin symbols whose last-trade timestamp
precedes the closing instant, and outcomes missed until a later session.
Recovering those records needs a separately witnessed historical target-close
price path. Corporate-action endpoints must remain bound to that same time.
Do not substitute today's quote or rewrite historical outcomes to fill those
gaps. Prospective immutable model/cohort pins, time-based evaluation with
purging/embargo, legacy-vs-witnessed cohort labeling, baseline comparisons and
out-of-sample interval coverage remain follow-up work. No model, trading gate
or rollout mode is activated by this repair.
