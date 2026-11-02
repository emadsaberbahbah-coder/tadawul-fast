# Decision v2 cash bases and execution presentation — 24 October 2026

Baseline: `70a88fa1a5dffda60925614c44293d3ecda08bfd`.
Component: `scripts/tfb_export_audit.py` 1.2.0 → 1.2.1.

## Confirmed scope and correction

The source snapshot contains a `Top_10_Investments_v2` header badge claiming
`EXECUTABLE`, an `output: HELD` status, and zero executable tickets. This is
a misleading presentation state. It does not establish that the backend
released executable orders; the view describes itself as read-only.

The `Portfolio_Decision_v2` budget difference initially suspected of being a
stale action string is **not a demonstrated arithmetic defect**. The view
explicitly declares two bases: an engine fixed reserve, and the Portfolio
percentage-of-NAV floor. Its KPI and narrative reconcile to those distinct
bases. Do not force the amounts to agree or choose the larger one as settled
cash. The clearer labels `ENGINE BUDGET` and `cash after the PF ... NAV floor`
are accepted by the new audit alongside legacy labels.

The uploaded `_Cash_Snapshot` record retains an old textual amount/FX note
while its converted-balance formula changed. Its current provenance and
freshness must be re-established from authoritative account evidence. This
revision neither rewrites historical notes nor certifies a broker balance.

## Implemented independent checks

- `portfolio_v2_research_cash_arithmetic` reconciles rendered cash, NAV,
  engine reserve, PF percentage floor, budget and narrative. The two reserve
  bases remain distinct. An explicit PF-floor narrative must use that basis.
- `top10_v2_execution_presentation` fails an `EXECUTABLE`/`BOARD: EXECUTABLE`
  badge over a zero-ticket or held/withheld board, and rejects any known
  board badge/output disagreement or positive tickets under a non-executable
  output. `BOARD: HELD` with zero tickets and a separate `FEED VERIFIED` label
  remain valid when the underlying output/count agree. Unknown states or missing/ambiguous KPI
  bands are `NOT_CHECKED`, never automatic PASS.
- Both checks run through the existing export-audit consumers. They add no
  funding, eligibility, score, publication, account or order behavior.

The prior export yields a display-arithmetic PASS and a Top10 presentation
FAIL. That arithmetic PASS is explicitly scoped to display calculations and
provides no account or execution clearance. Whole-SAR display comparisons use
a one-SAR rounding allowance; account certification tolerances are unchanged.

## Validation

`tests/test_decision_v2_cash_presentation.py`: 37 passed on the candidate.
The same tests run against the baseline auditor: 36 failed because the v2
checks were absent; the existing native-Portfolio isolation case passed.

Combined candidate command:

```text
python -m pytest -q tests/test_decision_v2_cash_presentation.py \
  tests/test_cash_snapshot_certification.py tests/test_export_audit_v1.py
115 passed
```

The unchanged export-audit deterministic selftest remains 50/50 with digest
`e4a1dd8650ad62f9`. Cases include both valid reserve bases, wrong arithmetic,
unknown and duplicated layouts, rounding, zero and held ticket states,
separate feed readiness, and the native `UNCERTIFIED` Portfolio view.

## Source and deployment limitations

At this baseline the repository supplies these five Apps Script sources:
`10_My_Portfolio.gs`, `11_Manual_Refresh_Coordinator.gs`,
`16_Decision_Top10.gs`, `24_Sync_Dispatch.gs`, and `26_Decision_Chain.gs`.
The current `Decision Pages v2 1.2.24` implementation, the owner of
`DP2_ENGINE_ENV`, and the writer/maintainer of `_Cash_Snapshot` are absent
from that mirror. Their actual bound filenames are not established by the
export; no replacement implementation is invented here.

To make persistent source repairs, retrieve the exact bound project and its
complete file inventory, save a restorable backup in authorized project
storage, modify only the verified owning files, and verify all unchanged
files after readback. A partial mirror must not be submitted through a full
`clasp push`. Current settled cash, reservations, positions, FX and their
timestamps remain independent evidence requirements.

This evidence records the repository audit fix. It makes no claim that this
revision installed bound Apps Script source, refreshed broker evidence,
authorized S1, or placed any order. Live label edits performed separately
must be supported by their own exact-range readback evidence.
