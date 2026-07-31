# Automatic Refresh Completeness v1

## Purpose

Separate **process success** from **data completeness**. A green sync is not sufficient evidence when only part of a page was freshly updated and the rest was preserved from an older run.

## Pages scanned in full

- Market_Leaders
- Global_Markets
- Commodities_FX
- Mutual_Funds
- My_Portfolio
- Insights_Analysis
- Data_Dictionary

Top_10_Investments and Portfolio_Decision remain decision-cockpit pages owned by Apps Script and are not written by the GitHub sync.

## Hard controls

- Exact schema-registry header contract.
- Minimum approved universe row count.
- No blank-symbol rows.
- No duplicate symbols.
- Freshness coverage measured across every row, not a sample.
- Name and price population thresholds.
- My_Portfolio must contain every ACTIVE ledger symbol.
- Active holdings must have positive quantity and cost.

## Initial thresholds

- Market pages: at least 95% of rows must carry a timestamp no older than 30 hours.
- My_Portfolio: 100% of active holdings must carry a timestamp no older than 8 hours.
- July 2026 minimum row-count defaults:
  - Market_Leaders: 1,025
  - Global_Markets: 6,512
  - Commodities_FX: 453
  - Mutual_Funds: 4,496

The row-count defaults can be raised through repository variables after an approved universe expansion. A lower count cannot silently pass merely because the workflow wrote some rows.

## Result policy

- Exit 0: all controls pass.
- Exit 1: warnings only; the workflow remains visible but does not report a hard refresh failure.
- Exit 2: one or more pages fail completeness, freshness, duplication, identity-universe, or portfolio controls.
- Exit 3: the audit could not run or could not prove the workbook state.

## Safety

- Read-only against the workbook.
- Does not call provider endpoints.
- Does not write, clear, sort, or format a business page.
- Does not participate in or interfere with the Apps Script manual-refresh lock.
- Production credentials are available only to scheduled/manual runs completed from `main`; pull-request tests use fixtures only.

## Rollout sequence

1. Merge the read-only audit and observe the first scheduled cycle.
2. Review every failed page and validate the thresholds against the approved universe.
3. Feed failed-page names into targeted automatic recovery.
4. Rerun the complete audit after recovery.
5. Declare the automatic cycle complete only when the final full-row audit passes.

The next phase must not replace last-good protection. It should refresh only the failed/stale page or symbol set and retain the working Manual Refresh priority controls.
