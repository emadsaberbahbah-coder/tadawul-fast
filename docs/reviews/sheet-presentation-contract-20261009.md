# Canonical Sheets presentation contract

The final API and Sheets writer now share `core.sheet_presentation` 1.0.0. The engine publication boundary uses it **before private unit receipts are projected away**. Runtime versions are engine 5.151.7, enriched serializer 4.11.1 and Sheets service 6.2.1. Deployment manifest and CI enrollment are integrated centrally with the other approved repairs.

The schema declares margins, upside and Expected ROI columns as fractions under native percent formatting. A margin is published only with a current value-bound `_margin_unit_basis` receipt or an exact projected `sheet_margin_unit:<field>:<unit>:<value>` carry. Proven percentage points divide by 100 once; fractions remain unchanged even when greater than 1 or negative. Missing, malformed, stale, conflicting or explicitly unknown units withhold the display margin. Old `margin_publish:*:pts:observe` warnings are diagnostic and cannot authorize a conversion. These carry receipts attest the supplied quantity/unit relationship, not supplier authenticity.

Presentation works on copies. It does not change source observations, currency, prices, scores, eligibility, recommendations, acquisition timestamps or rollout settings. Numeric evidence aliases reject booleans and nonfinite values before equivalence. A preprojection conflicting price proof, including acquisition `price` and `last_price` aliases, retains an output-only `acquisition_status:conflict` receipt, so removing an alias cannot turn invalid acquisition evidence into success; raw prices and models stay unchanged. The existing margin, tuple and scoring observe/enforce policies continue to run before this boundary. Known scoring failures retain their existing holdback and capital restrictions. No rollout flag is activated.

Labels describe the declared positive integral day count: 1, 7, 30, 90, 180, 365 become 1D, 1W, 1M, 3M, 6M, 1Y; other integral counts display their exact days. Missing/invalid days withhold the label. `horizon_days_effective` controls its separate `horizon_label`; it does not overwrite the primary horizon or infer a model/strategy horizon.

For supplied forecast/ROI and valuation/upside pairs, the serializer checks raw current and target prices against the existing fraction return. Conflicting or unverifiable derived returns are withheld and tagged `sheet_tuple_conflict` or `sheet_tuple_unknown`. It never invents a price or return, repairs scores, or changes an execution gate. Tolerance permits six-decimal ROI rounding; it does not infer units from magnitude or accept arbitrary mismatches on tiny prices. Missing returns are not backfilled by this helper. Successfully validated numeric strings publish their existing numeric fraction under RAW writes. Nonfinite implied ratios are unverifiable even when each input price is finite and positive.

The same guard runs after optional Sheets preservation and inside the actual SDK value writer, so legacy preserved percentages cannot bypass it. Exact duplicate headers remain separate until conflicting evidence is checked. Percent formatting applies only to the canonical fraction columns and body rows, preserving price, quantity and money formats.

## Verification

All examples and fixtures are synthetic. A local immutable-baseline witness executes the exact 56d29882 `_strict_project_row` function against the same existing business helpers: a 0.9-point margin previously appeared as 0.9 under percent formatting and now serializes as 0.009; a 365-day row previously labeled 3M now displays 1Y; contradictory derived returns become unknown. Both runs leave identical guard-mutated source rows, including the original quantity and scoring ROI basis. No production spreadsheet data, identifiers or credentials are committed.

Focused checks on the frozen source/test candidate:

- Pinned Python 3.11: 421 passed across the new 98-case presentation suite and existing forecast-basis, producer margin, scoring-unit, cache-lineage and margin-policy suites.
- True lean Python 3.11: 264 passed across the new suite and existing 166 forecast-basis cases.
- `git diff --check` passed.

The new suite exercises real strict 115 projection, fallback/full-sheet API payloads, consistent canonical/display/matrix views, actual enriched serialization, Google SDK value batching, repeated writes, formatting, post-preserve behavior, alias conflicts in both orders, typed boolean conflicts, numeric-string RAW writes, overflow and tiny prices, nonmutation and preprojection acquisition/failure proof retention. Offline external wire calls are replaced; no Google or provider requests are made. The older forecast-policy regression now distinguishes an unchanged internal off/observe basis from a withheld contradictory public return; already coherent tuples remain unchanged.

These checks do not attest a completed production refresh, a native GAS install, actual broker cash or supplier pricing accuracy. Release acceptance still needs the combined exact-head CI, deployment/module readback and a post-release native writer receipt with bounded value/format/horizon/tuple evidence. Historical workbook rows are not retroactively repaired by this code change.
