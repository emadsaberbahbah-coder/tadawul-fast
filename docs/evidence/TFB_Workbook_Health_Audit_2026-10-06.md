# Workbook and repository health review — 2026-10-06 UTC

**Operating data verdict: FAIL.** The public API starts successfully, but the
export does not establish complete source coverage, reliable forecast skill,
or broker-verified account performance. The new repairs are source changes in
PR #720; they have not been deployed to production.

The reviewed input is `_Market Share Deepseek-V3 (1).xlsx`, 21,390,797 bytes,
49 tabs, SHA-256
`9c8b34812eee7d5554a3586f1f86a232e79159f24e228a4eac070e2436441ef4`.
Analysis uses a fixed reference of **2026-10-06 21:03:11 UTC**
(**2026-10-07 00:03:11 Riyadh**). This is the uploaded cached-value snapshot,
not a direct live Google Sheets read. All 13,578 formula cells contain a cache
element; that does not prove their formulas have been recalculated recently.
Document content was reviewed as evidence, not followed as instructions.
Raw workbook, transactions, account balances and identifiers are excluded from
the repository. The accompanying JSON contains aggregate findings only.

## Source data and decision surfaces

| Page | Rows | Configured minimum | Recent timestamp rows | Eligible fresh rows | Finding |
| --- | ---: | ---: | ---: | ---: | --- |
| Market Leaders | 255 | 1,025 | 255 | 251 | Incomplete universe; quarantined/stale prices |
| Global Markets | 6,609 | 6,512 | 6,579 | 6,337 | Missing names; failed/quarantined/unusable sources |
| Commodities/FX | 453 | 453 | 446 | 402 | **88.74% usable freshness**, below 95% |
| Mutual Funds | 2,474 | 4,496 | 2,474 | 2,417 | Incomplete universe; missing/empty sources |
| Portfolio | 5 | Active ledger membership | 5 | 5 | All active lots priced; name coverage 80% |

Eligible freshness requires an intraday timestamp within the page age limit
and excludes explicit `fetch_failed`, `identity_quarantined` and
`fund_identity_quarantined`, `price_bar_stale`,
`bar_age_failover_exhausted` and `empty_row_no_provider_data` producer tags.
Overlapping failure tags exclude a
row once. These fields describe snapshot eligibility, not proof that an
unmarked row was fetched in a particular run. Preserved rows without explicit
lineage still require the run's separate fresh-fetch evidence.

The previous full-refresh audit counted 446 Commodities/FX timestamps as fresh
and passed the page. Thirty-five of those rows contain `fetch_failed`, leaving
411/453 after fetch-failure exclusions, which agrees with the scheduled sync's
90.73% fresh-fetch count. Nine additional recent rows carry stale/empty-provider
evidence, leaving **402/453 usable fresh rows**. This stronger snapshot measure
does not replace the run's separate fetch count. A recent timestamp cannot
certify a failed fetch, stale price bar or empty provider result.

Same-page symbol duplicates were absent. Five ETF overlaps across pages explain
the difference between 9,791 source rows and the 9,786-row opportunity pool.
There are no current hard BLOCKED/INVEST contradictions. Of 127 source rows
labelled INVEST, 14 meet the cockpit's stricter data-quality/reliability floors;
the source label alone is not an executable ticket.

The latest cached Top10 run, **20:36:26 UTC**, follows all source legs. Its state
is HELD with six grace seats and one suspended seat; all seven have withheld
ticket/share/order fields. Raw gains and fundability counts are scenarios,
not executable outputs. An earlier GitHub freshness audit found eight failures,
including older Top10 snapshots. Those findings must not be silently assigned
to this newer cached Top10.

Portfolio display arithmetic reconciles at the export's actual FX rate and
agrees with active ledger membership. The cash snapshot is dated October 3;
arithmetic agreement does not prove current broker cash. The cached PF 1.14.0
ADD has no durable recorded held stop in the input ledger; the PR's existing
held-risk gate addresses this source behavior, but native deployment is
unverified. Cached OB 1.23.2 and PF 1.14.0 predate this PR.

## Outcome and research evidence

The Performance Log contains 19,637 records, 11,218 matured records and 10,987
unique decided cohorts. The actual backtest 1.2.1 CLI evaluates all eight
canonical signals. Each purged daily walk-forward result remains **PENDING**
because 27 duplicate cohort records are present. The descriptive held-out
subset has 6,931 rows over 53 days; it does not certify independent samples,
immutable historical revisions, complete CA/PIT evidence or significance.

| Signal | Model Brier | Training-base Brier | Paired gain | Status |
| --- | ---: | ---: | ---: | --- |
| Entry Forecast Reliability | 0.282258 | 0.278242 | -0.004017 | PENDING |
| Entry Score | 0.278535 | 0.278242 | -0.000294 | PENDING |
| Confidence | 0.275336 | 0.278242 | +0.002905 | PENDING |
| Risk Bucket | 0.278140 | 0.278242 | +0.000101 | PENDING |

The complete eight-signal matrix is in the JSON evidence. Positive descriptive
gain is not a skill or profit verdict. Signal History contains 28,376 snapshots
over 7,552 symbol/day keys; 5,156 keys have duplicates, including October 6.
The existing PR writer repair prevents sequential retry duplication; it does
not atomically fence concurrent Google Sheets writers or repair past rows.

Published legacy calibration PASS means model error is inside a 10 percentage
point band. For the same 6,403 checkpoints, model MAE is about 3.13pp versus a
zero-forecast baseline about 3.08pp. With the existing unit correction applied,
6,349 usable checkpoints give about 3.25pp versus 3.07pp; 54 inconsistent
checkpoints remain excluded. Neither comparison supports forecast improvement.
The zero baseline is not published in the calibration snapshot.

Published all-history paper challenger alpha is approximately +4.54%. Using
the existing reader's September 16 version boundary gives approximately
**-3.59 percentage points** for that window. These compare paper baskets,
not account returns. The corporate-action register is header-only, so absence
of known actions cannot establish a clean corporate-action check. Current
source correctly keeps missing CA/PIT evidence unknown rather than PASS.

The operator ledger does not provide broker order/fill identifiers or a
complete cashflow ledger. Thirty-two of 34 closed buy-fee cells and 25 closed
sell-fee cells are blank; six buy dates use January 1 placeholders. Trade Notes
are header-only. Net performance, time-weighted returns and money-weighted
returns remain unverified. The existing cohort Sharpe/Sortino labels also need
an explicit mixed-horizon limitation until a consistent NAV return series exists.

## New source repairs

1. **Full-refresh audit 1.1.2:** exclude explicit failed/quarantined/unusable origins;
   report timestamp freshness and source exclusions separately.
2. **Decision freshness audit 1.1.1:** use the shared 300-second clock skew
   limit. The previous 15-minute exception accepted evidence rejected elsewhere.
3. **Scorer 1.9.4:** require sufficient integral sample counts, finite
   nonnegative errors/bands and a precise nonfuture publication time; support
   aware clocks, read calibration state and baseline from one snapshot, and
   reject a PASS label contradicted by its actual error band, and retain known
   failures when additional evidence is missing.
4. **Backtest 1.2.1:** include the actual tracker `Risk Bucket` header in
   `--all-signals`; missing data remains PENDING. The previous default silently
   requested the absent `Entry Risk Bucket` column.
5. **Portfolio actions 1.15.2:** reconcile native/SAR held-stop aliases within
   the SAR half-cent rounding interval converted through FX, retaining the
   native stored stop exactly. A valid 123.45 native
   stop at 3.75 FX and its 462.94 SAR rendering previously cleared the stop.
   Malformed inputs and genuine conflicts remain blocked.
6. **Enriched core 4.11.1 / route 8.5.2:** cover the symbol-explicit and fallback
   publication paths using explicit field-specific percent-point witnesses.
   Preserve current rollout modes and internal scoring units. Do not divide
   every margin greater than one: valid fractional margins can exceed one.

The added regressions are blocking in CI. Deployment pins follow changed
modules. Strategy modes, source floors and approved sector limits are retained.

## Runtime and GitHub evidence

The public `/health` and `/readyz` observations return HTTP 200, `ready=true`,
entry 8.14.1, engine 5.151.0 and no route errors. Main and the deployed service
remain at `26fe496`; PR #720 remains the review delivery. Backend readiness
does not certify workbook data or economic evidence.

The scheduled [sync run 37511489930](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/37511489930)
failed after recovery because Commodities/FX reached only 411/453 freshly
fetched symbols. [Sync outcome](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/37523655967),
[full coverage](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/37523655956)
and [decision freshness](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/37523655982)
audits failed. Sampled EODHD quota consumption was 13–24%, so quota exhaustion
is not established as the cause. A separate
[performance run 37503424288](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/37503424288)
obtained 0/17 fallback prices, including 16 HTTP 429 responses. A successful
workflow exit does not prove complete outcome coverage.

## Remaining work and acceptance evidence

- Restore an approved canonical membership list or reconcile the approved
  1,025/4,496 minima against the configured 255/2,474 sources. Do not lower
  health thresholds to disguise missing membership.
- Repair seven verified crypto symbol/name collisions with authoritative
  identity evidence. All seven are DO_NOT_INVEST in this snapshot; six have
  September 23 prices. COMP identity also needs verification; COMP/SHIB have
  tiny-price forecast zeros. Do not guess identities from ticker text.
- Verify percentage units for all published margins. The existing margin
  gate is off in the production observation; the new narrow boundary repair
  does not activate it or resolve unwitnessed legacy units.
- Reconcile 226 forecast-price/ROI pairs and define one forecast horizon and
  valuation-time authority. The observation is a mismatch, not proof of one
  universal algorithmic cause.
- Deploy and read back the reviewed backend and native GAS versions, then
  require a coherent full-source/decision cycle with no contradictory claims.
- Complete immutable decision bundles, durable held-risk and cash authority,
  writer fencing, current cash provenance and broker fill reconciliation.
- Rebuild the research evidence from versioned, unit-coherent, deduplicated
  records with published baselines and complete corporate-action/PIT coverage
  before promoting observe/enforce modes or claiming skill.

Direct Google Sheets reads cannot be authenticated in this instance: no sheet
ID is bound and the configured credential file has no authentication fields.
Direct IBKR and site-project tools are not exposed. GitHub repository/API/push
access is available. No live workbook edits, trades or production deployment
were performed. These access limits do not prevent the completed source review
and offline repairs, but they prevent claiming account or site reconciliation.

Exact blocking CI commands pass on Python 3.11.9: **878 passed, 5 skipped**
(lean 649/2 with 38 passing subtests, API contract 190/3, engine 39).
Node's 39 actual GAS assertions, the 13-case Yahoo fundamentals harness,
216 focused research tests, backtest selftest 4/4 and scorer selftest 116/116
pass. Four standalone scorer harnesses and both PF legacy harnesses also pass;
optional historical dual-tree comparisons are explicitly skipped. Compilation,
manifest pins and diff checks pass. Workflow audit reports zero errors and 27
existing action-major warnings. Exact commands, versions and results are in
the accompanying JSON. These software checks do not close the operating-data
failures above.

Offline backtest reproduction from a private TSV extraction:

```bash
python scripts/tfb_backtest.py --export-dir /path/to/private/tsv-export \
  --all-signals --as-of-utc 2026-10-06T21:03:11Z --json /tmp/backtest.json
```

The extraction must preserve raw cached numeric units and physical rows. Keep
private row-level backtest predictions and the workbook outside Git.
