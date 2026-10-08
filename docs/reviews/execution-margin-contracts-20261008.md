# Execution replay and margin units — 8 October 2026

This batch builds on merged `f1ab04488b804f2ae907d2ff89a06b583cfd6017`.
It implements the offline accounting boundary of TFB-03 and the supplier-unit
boundary of TFB-05. The attached audit is a source of proposed work and evidence;
it does not authorize orders, native ledger amendments or production deployment.

## Exact execution replay

`core/execution_accounting.py` and `scripts/tfb_import_executions.py` replay a
captured JSON export without broker or workbook access. Monetary JSON numbers
are parsed as Decimal; decimal strings also work. The minimal input is:

```json
{
  "trades": [
    {
      "trade_id": "synthetic-fill-1",
      "account_id": "synthetic-account",
      "symbol": "EXAMPLE",
      "currency": "USD",
      "side": "BUY",
      "size": "100",
      "price": "12.71",
      "commission": "1.99",
      "commission_currency": "USD",
      "trade_time": "2026-09-24T13:52:01Z"
    }
  ]
}
```

Run against private paths with existing output directories:

```sh
python scripts/tfb_import_executions.py snapshot.json \
  --state replay.json --report reconciliation.json
```

If the export omits account IDs, supply an explicit `--account-id`, or an
`--account-map` JSON object mapping captured trade IDs to account IDs. Existing
account context must agree. No instrument suffix or account identity is guessed.
The module docstring describes optional original-row, cost-reference and
amendment-request input contracts. Complete original rows and their fingerprints
are required for conditional proposals; no proposal changes a native ledger.

Repeated account-scoped trade IDs are idempotent, while conflicting records are
refused. Raw payloads and source fingerprints are preserved. Historical cost
references and proposals retain their original execution cohort and reference
lineage when later executions or references arrive. Fingerprints prove internal
consistency, not broker authenticity.

The state is replaced atomically under an exclusive POSIX local lock. The report
is a digest-bound, regenerable sidecar. Exit 2 means no state was accepted; exit 3
means state was accepted but the report may need regeneration by repeating the
same input. Invalid input leaves existing state and report unchanged. This is not
a multi-file transaction or a distributed writer protocol.

Read-only broker evidence corroborated the DDI split fills and SBAC execution
date. Private broker/account/order IDs are excluded from this change; test IDs
are synthetic. The DDI arithmetic is:

| Component | USD |
| --- | ---: |
| `100 × 12.71 + 129 × 12.72` principal | 2911.88 |
| Reported commissions `1.99 + 2.5671` | 4.5571 |
| Principal plus reported commissions | 2916.4371 |
| Audit total-cost reference | 2917.1207 |
| Residual awaiting charge classification | 0.6836 |

The broker's `net_amount` is retained raw and does not prove a fee-inclusive
total. Reported commissions remain unreconciled broker fields. Explicit fee
currency must agree with execution currency; no FX conversion is inferred.
The capture contract treats an omitted commission currency as the execution
currency. It does not establish the charge classification or a total fee
statement. Aggregation remains separated by account, symbol and currency.

TFB-03 is partial: approved instrument mapping, authoritative original native
rows, residual charge classification and a separate native application adapter
remain open. SBAC's synthetic amendment witness preserves the original date and
proposes 24 September from the captured execution timestamp. No accounting
write, historical backfill, FIFO or realized-income calculation is performed.

## Margin source contract

The scorer consumes value-bound unit receipts for gross, operating and profit
margins. Proven fractions and percentage points convert explicitly, including
negative values and fractions greater than one. Explicit unknown, malformed or
stale receipts cannot fall through to magnitude guessing. Historical rows that
never carried a receipt retain the existing compatibility parser.

Receipts travel independently of public unit publication mode and include raw
source lineage when available. Publication and scoring use the same economic
unit. Replacing a margin invalidates the previous receipt, even when the new
number happens to be equal. Coherence comparisons respect the proven unit.

A read-only public EODHD AAPL demo response provided this source witness:
`Highlights.ProfitMargin = 0.2762` and
`Highlights.OperatingMarginTTM = 0.3262`. The latest four reported quarterly net
income/revenue totals yielded `0.2761860491021222`, which rounds to the native
profit-margin value. The operating figure has a different basis and is not
claimed to match that quarterly calculation exactly.

Fresh EODHD native `ProfitMargin` and `OperatingMarginTTM` scalars are fractions.
Explicit percent strings convert once. `OperatingMarginTTM` takes precedence
over the nonstandard alias. Nonstandard numeric `GrossMargin` and
`OperatingMargin` aliases retain their configured legacy conversion with an
unknown unit receipt. Computed income/revenue ratios receive fraction receipts.
Yahoo ratios receive their declared source-unit receipts.

Earlier engine fundamental-cache entries can contain a value-bound receipt from
the incorrect native conversion. Cache reads therefore quarantine margins from
an older or missing margin-contract version as explicitly unknown. Non-margin
fundamentals retain their existing cache behavior. Fresh acquisition under the
current source transform restores margin proof; merely writing an old entry back
to Redis cannot make it current. This prevents a warm cache from reviving the
previous conversion after release.

This does not complete all of TFB-05: the audit's 65 historical workbook forecast
tuples were not independently replayed, and the broader stage-verdict contract
remains open. Funding modes, calibrated reliability floors, cash floors, position
caps, sector caps and forecast thresholds retain their existing policy.

## Validation boundary

Focused tests cover supplier-to-scoring behavior, cache receipt transport,
explicit uncertainty, exact accounting replay and real CLI file preservation.
The new accounting tests join the required lean CI lane; new supplier/scoring
tests join the required full-dependency lane. Deployment version pins track the
changed modules and new CLI. Exact local CI results are recorded in the draft PR
after final source stabilization.

Provider I/O in regression tests is stubbed. Local tests do not attest live
workbook integrity or establish an execution-ready accounting correction.
The approved production deployment of base `f1ab044` is separate from this draft.
