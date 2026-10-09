# Global Markets scoring-block review — 9 October 2026

## Current evidence

The read-only workbook census at 11:01 UTC (14:01 Riyadh) contains 6,609
Global_Markets symbols. The latest completed market writer is sync 6.64.14,
at 01:27:57 UTC. This is publication evidence, not an assertion that every
quote is executable at the review time.

| Observation | Current count |
| --- | ---: |
| BLOCKED / WATCHLIST / INVESTABLE / empty status | 1,141 / 5,262 / 84 / 122 |
| Positive price / missing or nonpositive price | 6,350 / 259 |
| F7 non-convergence, reason `pass_limit` | 2,202 |
| F7 error, reason `source_inputs_changed:provider_rating` | 53 |
| Blank issuer name | 260 |

All 2,202 pass-limit rows have positive prices and withheld overall/opportunity
scores. They include both blocked rows and watchlist rows: a watchlist status
can preserve a protective exit while still denying new investment. All 53
provider-rating mutation errors lack a positive price. Repairing their error
classification must not make those instruments investable.

The completed writer acquired 6,285/6,609 rows (95.0976%), passing the existing
95% acquisition guard. The formal full-coverage failure is issuer-name coverage
(96.066% against 99%). Of the 260 blank names, 259 also lack a usable price.
Provider logs contain symbol-specific failures, without evidence of exhausted
quota or a global circuit-breaker defect. A ticker label is not a substitute for
an issuer name.

## Responsible code and confirmed causes

`core/data_engine_v2.py` owns the canonical scoring/Phase-DD pair, F7 settlement,
recommendation provenance, and investment holdback. The original
`_cap_intrinsic_display` repeatedly compresses an already compressed value.
F7 feeds that displayed value into the next model pass, so valuation and forecast
inputs change despite unchanged source facts. A reproducible, complete synthetic
equity with price 100 and raw intrinsic 300 compresses to 140, 138.1606, 137.3427,
then 136.8704; the existing four-pass settlement limit is exhausted.

`_classify_recommendation_8tier` also writes the sources `price_unavailable` and
`fixed_income_sukuk`, but the engine-owned provenance set omits both. A later
classifier pass can capture the engine's own HOLD as an upstream provider
rating, violating F7's frozen-source contract. The current Global Markets
rating-error witness is the missing-price case; the sukuk case is independently
reproducible and uses the same provenance helper.

Removing that false failure exposes a separate portfolio display seam: a held
position with an old positive position value can derive a drift-based ADD even
without a current usable price. A narrow final output guard converts only
no-price ADD labels to HOLD, in both normal and incomplete-FX derivation paths.
Position facts and weights are retained; valid-price additions and protective
exits keep their existing behavior. This is not a change to the portfolio
allocation or broker-execution rules.

`core/scoring.py` consumes intrinsic value and upside; it is not changed in
this repair. `scripts/run_dashboard_sync.py` owns workbook refresh and preserved
fetch-failure rows; it is not changed. Provider routing and the existing price,
identity, coverage, freshness, and investment rules retain their thresholds.

## Repair constraints

Keep the current display-cap policy, model weights, F7 tolerances and pass limit.
Maintain a value/source-bound raw model basis during settlement and apply the
display transformation idempotently. Only witnessed raw values may be recovered;
an old warning or stale display alone is insufficient. Preserve off/observe
publication behavior and the exact 115-column market schema.

Recognize the two explicitly engine-written recommendation sources without
inventing provider ratings. Continue withholding scores and new capital on
settlement errors, oscillation, missing price or unproven inputs. Known failed
rows require a genuine fresh provider rebuild; switching modes or deleting a
warning is not recovery. Cache/persistence review confirms that a successful
fresh Global Markets provider factory starts from new source data, while failed
fetch preservation retains the prior failure.

## Verification and rollout

Require actual canonical scoring/Phase-DD regressions, repeated-cap and changed
source/value/policy tests, missing-price and fixed-income provenance tests, and
the existing F7 fail-closed suite. Include no-price/valid-price portfolio action
and protective-exit controls and the actual blocked funding boundary. Keep
these in required CI and pin engine 5.151.9 in the deployment verifier.

Deploy only after the exact candidate passes required CI. Confirm the deployed
commit and engine version in live health, and run the existing authenticated
readback when workflow dispatch is available. Before any newly initiated market
write, verify a workbook backup and use the existing single-key Global Markets
refresh under the production writer lease. Re-read the symbol-set, statuses,
structured F7 errors and sample rows. Report residual provider/data/policy blocks
separately; neither a passing acquisition guard nor a cleared calculation error
certifies an investment decision. No broker actions or portfolio ledger changes
are part of this repair. Reverting the engine patch and deployment pin rolls
back the calculation change; workbook data recovery uses the verified backup.

At 11:19 UTC the shell GitHub credential expired. Connected GitHub code/PR tools
and Render deployment remain available, but the connected app exposes no
workflow-dispatch operation. The legitimate fallback is the existing regular
writer, scheduled at 12:17 UTC (15:17 Riyadh), whose start may be delayed by
GitHub. Re-running the older scheduled GM job is unsuitable for a controlled
market-only refresh: it also repeats Performance recording and dependent
recovery. Do not change the schedule or bypass those safeguards to force it.

Backup run 37922762113 completed successfully at 11:17:29 UTC: the exported
22,466,693-byte XLSX (SHA-256 prefix `b5762ae11558`) was uploaded to OAuth Drive.
Read-only metadata confirms matching stored size/time. This is export/upload
evidence; a full stored-checksum comparison and restore test are not attested.
