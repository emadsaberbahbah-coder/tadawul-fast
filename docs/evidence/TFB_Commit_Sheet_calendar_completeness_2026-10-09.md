# Calendar completeness and dated annotations — 9 October 2026 Riyadh

Base: `56d29882b0dd19e6594fcdf74d43335cf7fed6c1` (#740).
Branch: `codex/calendar-completeness-20261009`.

The read-only review found the selected canonical symbol `GRT-UN.TO`
rejected by the calendar harvest shape guard. It also reproduced the actual
native reader omitting 185 known earnings rows from a 384-row calendar,
because it read `A1:H200`, and keeping yesterday's countdown after Riyadh
midnight. The calendar tab has seven columns. No issuer date was disproved
or replaced by this repair.

## Resulting behavior

`scripts/run_calendar_sync.py` advances 1.1.2 → 1.1.3. Venue suffixes remain
mandatory, while bounded hyphenated/dotted class roots are admitted in both
harvest and prior-event routing. `GRT-UN.TO`, `BRK-B.US` and `BRK.B.US` pass;
section headings, counts, unsuffixed futures and malformed separators fail.
The existing ticker-guard disable setting retains its existing meaning.

The producer reads the seven-column prior calendar across the allocated
grid, rather than only 400 rows. The supported bound is 5,000 body rows plus
the header. An allocated grid above that bound, an oversize returned/merged
table or an unreadable prior table prevents publication before any update
or clear. A genuinely missing worksheet can still be initialized. Rejecting
an oversized allocated grid is intentionally conservative: a bounded prefix
cannot prove that later blank-separated rows contain no known events.
When the new compacted table is shorter, replacement clears through the full
established prior extent, so old rows beyond 1,000 cannot survive as duplicate
unmarked observations or expired/junk tail rows.

Missing earnings or ex-dividend facts remain blank, with blank countdowns.
The Source column adds an explicit derived `[events unknown:...]` annotation;
it does not claim an error classification or a successful provider lookup.
Repeated carries do not duplicate this annotation. When the missing field
becomes known, the old derived status disappears. Fresh invalid or expired
dates are revalidated at row publication. Original carried date, source
and observation time remain preserved; unknown observation times are not
replaced with the current time. The existing seven-column schema is retained.

The native `dt10EarningsMap_` reads the actual used row count and at most the
seven contract columns, within the same 5,000-body-row bound. It includes
late rows and never requests nonexistent column H on a seven-column grid.
An oversized or unreadable table yields an empty annotation map instead of
silently presenting a truncated prefix.

`dt10EarningsMapFromValues_` derives the countdown from the strict event date
and the current Riyadh day. The optional second argument is a literal ISO
date or a Date instant for deterministic tests; an invalid explicit argument
fails safe. Date cells/instants use Riyadh UTC+3. ISO text must be a complete
valid Gregorian date. Missing, malformed and past event dates are omitted,
even when their stored Days To Earnings value is positive or zero. Header
columns must appear on the same row. The existing native self-test now uses
coherent fixture dates and a fixed review date.

This feature remains an earnings annotation. The repair does not introduce
an event trading gate, new dates, provider endpoints, subscriptions, news
bundle activation, or changes to verdict/sizing/funding logic.

## Verification

All checks ran offline against the actual source methods/helpers with
synthetic service boundaries. There were no live workbook writes or provider
requests.

| Check | Result |
|---|---|
| Python 3.11 lean: sticky + completeness suites | 49 passed, 1 skipped |
| Contract environment: same suites including actual provider routing | 50 passed |
| Actual native helper completeness harness | 25/25 passed |
| Calendar script offline self-test | 9/9 passed |
| Full-source board funding and containment harness | 64 passed |
| Existing P-145 grace-sizing T1–T5 battery | PASS ×3, digest `f0a0b50926b7f4bd` |
| Python compile and `git diff --check` | passed |

The lean skip is only the actual provider module's `httpx` dependency. The
contract run executes that case and confirms `GRT-UN.TO` remains the same
canonical EODHD request code and Yahoo calendar symbol. No provider source
change is necessary. The native suite checks midnight rollover, strict
dates/leap years, date cells, missing facts, complete late-row coverage, exact
capacity boundary, read errors, no mutation and annotation idempotence.
An independent producer review reproduced and fixed the old replacement-tail
defect; a stateful actual-publication fixture now verifies expiry/junk removal
and unique carried events after a 1,101-row table shrinks.

## Integration requirements and limits

The candidate sets the shared Apps Script module version to 1.13.0 as
coordinated with the parent (calendar, holdings and readiness hunks are
independent). The parent integration owns the verifier pin for calendar sync
1.1.3 and the other joint native hunks. This isolated local commit intentionally
does not edit parent-owned workflow/version-pin files. Add the new Python suite and
native harness to blocking CI; run the actual-provider routing case in the
contract environment with `httpx`.

Existing provider source strings remain coarse and are not expanded into a
new per-field provider receipt schema here. The change does not certify any
issuer announcement, calendar provider accuracy, deployment identity or
investment profitability. Production readback is required after an approved
deployment; no deployment is performed by this candidate.
