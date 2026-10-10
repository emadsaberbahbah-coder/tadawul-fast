# Decision publication update — 10 October 2026

Base: `af885d8df2f5c754b4cdf3aaba9a88310617bc96` (main, native Top10 1.13.2).
All times below are Riyadh unless stated otherwise.

## Problem and behavior

The public freshness audit reported that the 03:16:58 Top10 board preceded
the final Mutual_Funds and Global_Markets publications. Its status described a full universe
despite 255 leaders against the 1,025 floor and 2,474 funds against 4,496.
The existing audits correctly rejected the workbook. This update preserves
those requirements and prevents incomplete evidence from funding a board.

- **Native Top10 1.13.3:** capture source receipts before reading the pool;
  require complete, current acquisition evidence and the four existing row
  floors, with one shared GitHub sync run across all four pages; recheck the
  captured generation before allocation and final painting.
  Unready or changed sources freeze confirmation memory and withhold financial
  cells. Status describes available Sheets rows. An empty Sheets pool renders
  an empty research result without fetching a different backend universe.
- **Final publication guard 1.0.0:** after the entire sync matrix and inline
  recovery, reuse the coverage and decision freshness audits under the same
  workflow write lease. Rejected or unavailable audits downgrade only the
  bounded `_Status` decision-feed key, with exact readback. Passing checks never
  promote the feed. Both auditors are 1.2.2; the final guard passes an explicit
  policy that cannot lower floors or freshness requirements and binds source
  receipts to the workflow run. No Python job refreshes a native cockpit.
- **Shared quote validation 1.0.2:** check every provider alias, including
  `primary_provider`, and reject supplier quote clocks that conflict with the
  successful acquisition receipt. Quote time remains distinct from retrieval
  time. Evidence survives the native pool projector unchanged.
- **Portfolio actions 1.15.1:** debit exact price × shares × FX from cash,
  reserve, position and sector budgets. Whole-SAR display rounding cannot fund
  another ADD. The existing fee switch controls fees independently.
- **Calendar sync 1.1.4 / provider 1.2.2:** publish header, rows and old blank
  tail in one RAW request rather than clearing the prior calendar first. A lost
  acknowledgement is unconfirmed and requires readback before retry. Yahoo
  fallback fills either missing event field independently.
- **CI 1.1.3 / verifier 1.0.44:** make the new regressions blocking, cover
  Apps Script-only pushes, and pin the updated runtime constants.

## Verification

Tests use the real public portfolio/opportunity builders, the complete native
Apps Script and mocked remote transports. Adversarial fixtures cover exact cash
boundaries, conflicting provider/quote evidence, deficient and changed source
receipts, replay/render races, empty input, calendar write failures and partial
event data. Existing acquisition, reconciliation, calendar and funding checks
remain enrolled. The final-publication tests exercise bounded Google Sheets
writes and the actual workflow shell blocks.

Integrated local validation: **623 Python tests and 46 subtests passed** in one
focused run, plus **150 native checks** (45 readiness, 64 board funding,
25 calendar, 16 reconciliation inputs). Compilation, workflow parsing, shell
syntax and manifest pins passed. These counts exclude earlier overlapping runs.

The local managed environment cannot establish production deployment or native
installation. Its installed web framework differs from the production pin;
the PR's existing production-pinned CI lanes remain required. Locally, the
funding oracle uses identical input bytes through file stdin because managed
process pipes stall; the checked-in test and GitHub runner command are unchanged.

## Coverage recovery remains required

The [public audit recovery requirements](coverage-recovery-evidence-20261010.md)
record the deficits and rejected decision timing. This patch supplies no
approved restoration roster. The 770-row leaders deficit and 2,022-row funds
deficit do not identify the missing instruments. Public audits also reject
Global_Markets name coverage of 96.05% and Mutual_Funds coverage of 98.02%.

Restore membership only from a reviewed authoritative roster, with a backed-up
symbol, exchange and currency diff. Verify current issuer names before filling
gaps; historical labels cannot override identity quarantine or acquisition
failure. Keep the 1,025 / 6,512 / 453 / 4,496 floors and 99% name requirement.

## Release acceptance

1. Merge only after the blocking PR checks pass; deploy the reviewed backend
   revision and verify the runtime manifest.
2. Install the complete `apps_script/16_Decision_Top10.gs` 1.13.3 in the bound
   workbook and run `dt10SelfTest`. Repository changes alone do not install it.
3. Finish verified membership/name recovery and run a complete source sync,
   including inline recovery and final publication evidence. Existing coverage
   failures are expected until this recovery is complete.
4. After all sources finish, run the native `refreshDecisionTop10`. Check its
   final versioned run witness, source readiness, available pool counts and
   withheld/executable fields. The Python workflow cannot trigger this function.
5. Rerun Full Refresh Coverage and Decision Surface Freshness against that
   final workbook. Use strict execution acceptance when evaluating a release;
   an informational green workflow or observe token is insufficient.

The final-publication blocker is deliberately sticky: the finalizer never
clears or promotes it, and native Top10 cannot ignore it. Recovery therefore
requires an operator-controlled release after the underlying failures are
fixed. First prove full source coverage, the same four-page sync cohort and a
current accepted Portfolio_Decision. Refresh the new native script while the
feed remains blocked, and read back that all old financial ticket fields are
withheld. Review the finalizer evidence and confirm that only the prior Top10
surface remains blocked. Only then republish the upstream feed for the verified
source run and immediately refresh/read back the native board and rerun both
audits. Do not clear a coverage failure, unknown cause or Portfolio failure;
do not disable the verdict gate. No connected tool in this session can execute
that native release or certify its result.

This patch changes no live workbook membership, lowers no approval floor and
does not provide broker custody, cash or execution evidence. Those existing
execution requirements remain applicable after data recovery.
