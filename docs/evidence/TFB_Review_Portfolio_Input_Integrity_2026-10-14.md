# Portfolio input integrity repair — 14 October 2026

Two source defects affected portfolio data quality. The portfolio-actions route
ignored a native caller's incomplete-read declaration, and the workbook reader
normalized monetary `Unrealized P/L` and percentage `Unrealized P/L %` to the
same key. The latter percentage overwrote the amount whenever upstream math
could not recompute it.

The route now withholds execution for missing/non-list `rows`, filtered or
capped rows, caller boolean `true`, or any supplied malformed completeness
flag. Only literal boolean `false` clears the optional caller declaration;
it cannot override backend truncation. Legacy callers with a valid row list
and no declaration retain their behavior. A complete explicitly empty custody
packet plus an explicit empty row list remains valid. The route's private-free
holdings metadata reports `input_incomplete`.

RowsReader 1.0.2 preserves percent units in header matching. Exact header
meanings take precedence; prior unit-omitting aliases remain only when the
schema assigns them one meaning. Duplicate symbol, quantity or cost-basis
columns refuse manual holdings. Duplicate optional observations stay absent
instead of selecting a conflicting column. Reordered real registry columns,
missing recomputation inputs and case/punctuation aliases are covered.

Validation: the new route/unit regression fixtures failed before the repair
(`23 failed, 10 passed`). The final expanded portfolio, reconciliation, fee,
schedule, schema and presentation suite passed (`482 passed, 2 skipped`).
`git diff --check` passed for the changed implementation and existing test
files. Fixtures use fictional holdings, accounts, cash and quote receipts.

This repair supplies no custody or cash evidence. Existing 900-second capture
checks, settled-minus-reserved funding, quote acquisition/freshness checks,
no-funding-from-proposed-sales behavior and research/execution separation are
unchanged. The separate native Portfolio_Decision caller must still forward
fresh account evidence; it is absent from the partial repository mirror.
