# Decision readiness repairs — 9 October 2026 (Riyadh)

This release implements the investment-data review findings at the actual
portfolio, allocation, serialization, calendar and outcome-writing boundaries.
It distinguishes reviewable research from execution readiness. A recommendation
or a successful refresh alone cannot establish safe funding or forecast skill.

## Implemented behavior

| Boundary | Required evidence and resulting behavior |
| --- | --- |
| Portfolio actions and new allocation | Complete account/instrument position matching, settled cash less complete reservations, fresh FX and witnessed holding quotes. Missing, stale or inconsistent inputs withhold funding. Portfolio rows become BLOCK with no order levels, NAV, weights or proposed proceeds. |
| Sold or partially sold holdings | Exact quantity differences produce private conditional review proposals. Missing positions remain unknown. The release does not rewrite purchase ledgers, infer fees or credit settlement cash. |
| Signed board replay | The signature binds the frozen research inputs. Current position/cash/FX certification is checked again at allocation; a still-valid signature cannot preserve expired financial evidence. Unverified empty pools publish zero money. |
| Market freshness | Uses successful acquisition evidence and the actual market quote time. Retrieval time, preserved rows, conflicting aliases or a disabled policy switch cannot establish a successful quote. Older prices additionally need a confirmed exchange session. |
| Currency sizing | Supplied row FX cannot replace a fresh certified currency rate. Missing foreign-currency evidence blocks native quantity sizing. |
| API and Sheets presentation | Value-bound margin units convert percentage points to fractions once. Unknown units and conflicting derived returns are withheld. Horizon labels follow declared days. RAW writes retain valid numeric types; source prices and scoring inputs remain unchanged. |
| Calendar producer and native reader | Reads the complete bounded calendar cohort, preserves source facts, recomputes countdowns from dated events, rejects incomplete reads and clears stale publication tails. Share-class symbols remain recognizable. Missing events are unknown. |
| Prospective performance outcomes | A target-session closing-price witness is required. Failed, preserved, conflicting, intraday or later-session prices cannot become a nominal outcome. Corporate-action adjustments stop at the target session. Existing matured records are unchanged. |

## Capture and native installation

The reconciliation packet is **declared evidence**, not authenticated broker
origin. Its account scope, completeness, timestamps, quantities, cash and FX
must be obtained by an authorized capture process. The new offline CLI produces
safe counts and an optional private review report; it never contacts a broker
or workbook. See [the reconciliation contract](../portfolio_reconciliation_contract.md).

Native Top10 source version 1.13.0 forwards same-row holding proof and the
private document property `TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1`. Missing or
invalid property data withholds execution. A coordinated native installation
and a fresh legitimate capture are required before executable recommendations
can resume. Native portfolio-decision source is absent from this repository;
that caller also needs the packet. Repository changes do not attest either
native installation or repaired historical workbook cells.

## Verification and remaining evidence

Required CI includes real allocation and authenticated route regressions,
account/cash/FX reconciliation, native full-source transport, complete calendar
reads, presentation roundtrips and production-calendar outcome witnesses.
Independent reviewers checked the coordinated source and its blocking cases.
GitHub checks on the final commit and deployed readback establish release
identity; they do not establish real broker quantities or forecast skill.

The following remain outstanding after the code repair:

- An approved versioned Market_Leaders and Mutual_Funds roster. Existing coverage
  floors are retained. No symbols are manufactured, removed or restored by this
  release; any membership change requires a backup and an approved concrete diff.
- Authoritative complete custody, settled-cash, reservations and native fee
  evidence. Conflicting historical purchase fees and sold-position ledgers need
  reviewed reconciliation, rather than guessed arithmetic.
- Native installation and live workbook verification of value, formatting,
  horizon and recommendation consistency after a controlled refresh.
- A sufficient prospective outcome cohort, including target-close recovery for
  missed observations, before calibration or forecasting changes can be judged.
  Legacy mixed outcomes remain unsuitable as proof of forecast skill.
- Broader news/Insights cohort and recommendation research identified in the
  review. This release does not activate new providers, paid subscriptions,
  model modes or enforcement flags.

No trade, broker order, financial ledger amendment, roster restoration,
credential exposure or repository-visibility change is part of this release.
Rollback should preserve research-only operation until the inputs reconcile.
