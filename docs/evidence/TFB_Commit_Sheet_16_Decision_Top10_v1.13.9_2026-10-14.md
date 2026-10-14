# Top-10 v1.13.9 — entered cash and UTC holding timestamps

The first cash KPI displayed `Deployable (SAR)` while the final publication
feed withheld sizing and no custody/cash reconciliation packet was present.
It now says `Entered cash (SAR)` on both new and existing layouts. The amount
and operator input remain the same; the title never claims settled funds or
permission to trade.

The holding reader also missed the workbook's canonical `Last Updated (UTC)`
header. It now transports that cell as `last_updated_utc`, preserving a stored
Date's instant and the original UTC basis for a timezone-naive string. Generic
`Last Updated` retains its existing `last_updated` field. Multiple accepted
source timestamp columns make the holding input incomplete and omit timestamp
proof. A display-only `Last Updated (Riyadh)` companion is not a second source
proof. No time is invented or replaced with the refresh time.

Cash values, manual controls, funding allocation and release/feed behavior are
unchanged. This update does not resolve absent account statements, missing
Portfolio_Decision consumed-source receipts or the final audit's source
identity blocker.

## Changes

- `apps_script/16_Decision_Top10.gs`: version 1.13.9, first KPI title, one
  corrective title write during render, UTC holding header/field transport and
  ambiguous timestamp proof rejection.
- Native reconciliation harness: full-source tests for UTC strings and Date
  transport, ambiguous source timestamps in either order, UTC plus Riyadh
  display companion, and updating an existing cash title under both feed
  states with unchanged cash/zero invented allocation.
- Existing native harness version pins updated. Historical feed guards use a
  reconstructed, immutable v1.13.8 source: only the exact enumerated v1.13.9
  edits are reversed, and the entire result must match SHA256
  `9d11832acae738be2299faa463ca6aa71b8e9a77ab31c05d4bf8d5e55bcd506c`.
  Unrelated code and altered UTC transport fail this digest check. Original
  D1–D5/G1 guard logic and its four negative mutants are retained.

## Validation

Current GAS SHA256:
`2e0967dd76bb260832eba1053ce62b682d3dc9ecd4426f9580c363663860ecca`.

`node --test tests/js/apps_script_syntax.test.js
tests/js/portfolio_reconciliation_inputs.test.js
tests/js/decision_board_funding.test.js
tests/js/decision_source_readiness.test.js
tests/js/dt10_feed_cohort_binding.test.js
tests/js/decision_input_truth.test.js` passed: three native sources compile;
20 reconciliation-input checks; 64 final funding checks; 73 readiness checks;
21 current feed/history checks; 20 input-truth checks. The existing three
optional input-truth comparisons against v1.13.3 were skipped because that
external base fixture was not supplied.

With the actual v1.13.7 source from commit `dda1211` supplied as `DT10_BASE`,
the feed harness passed **27/27, zero skipped**. This includes current feed
behavior, immutable-source and mutation controls, historical default-off
renderer/refresh comparisons, D5's four mutation negatives and G1's thirteen
discriminating negatives.

## Bound-project installation and readback

Repository implementation is ready; the bound Apps Script deployment has not
been installed or claimed verified by these offline checks. The repository
mirrors only three files and does not contain the native portfolio caller.

1. Use authenticated Apps Script access to export the **complete** bound
   project, manifest and current Script/Document Property configuration before
   any update. Keep the private reconciliation packet private.
2. Replace only `16_Decision_Top10` with this complete v1.13.9 file in the bound
   project. Preserve every unrelated bound file and property. A full clasp
   push from the partial repository mirror would delete unrelated files.
3. Run `dt10SelfTest` and verify the logged version is 1.13.9 with no failed
   core checks. After the source sync completes, run the native
   `refreshDecisionTop10` through authenticated execution.
4. Read back A15 as `Entered cash (SAR)` and confirm its A16 amount matches
   the entered amount. Read B2 for exactly one `pool_reconciliation_v1` token
   and one valid source-readiness receipt for the consumed source run. Inspect
   the actual request's canonical holding UTC fields without exposing private
   reconciliation evidence in logs.
5. Re-run strict final publication verification after both native cockpits
   consume the source cohort. Keep missing source identities, missing PF
   consumption proof and custody evidence as blockers. Installation or a
   recent display timestamp alone cannot issue executable permission.

Rollback is replacing only this file with the backed-up v1.13.8 source.
Re-render the native cockpit; no funding state or evidence property is changed
by this update or rollback.
