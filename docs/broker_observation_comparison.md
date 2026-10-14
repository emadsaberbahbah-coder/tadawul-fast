# Offline broker observation comparison

`scripts/compare_broker_observation.py` compares saved IBKR tool observations
with the exported workbook without contacting the broker or editing inputs.
It reads serialized XLSX numeric cells and JSON numbers as exact decimals.
Native-currency cash, market values and average costs remain in their own
currency. A USD summary is compared to detailed position values only when
every observed position is USD.

The observation JSON contains saved structured tool results under `positions`,
`balances`, `summary` and `orders`. The direct shapes are:

```json
{
  "retrieved_at_utc": null,
  "source_asof_utc": null,
  "account_id": null,
  "positions": {"positions": []},
  "balances": {"balances": []},
  "summary": {},
  "orders": {"isError": true}
}
```

MCP `structuredContent` wrappers are also accepted. A failed or missing orders
result means coverage is unknown. An observed empty order list differs from
an unavailable result, but neither supplies complete account commitments or
reserved cash. Matching LIMIT/STOP descriptions and remaining quantities are
reported only as possible protective alternatives; OCA linkage is unproven.

Pass an explicit private contract mapping, for example
`{"123456":"SYNTH.US"}`. Each key must be an exact positive broker contract
ID, and each value must be an uppercase canonical symbol in the workbook.
Explicit mappings are labeled `explicit_contract_mapping`. Without a mapping,
only a unique ticker/currency match is linked and it is labeled
`provisional_unique_ticker`. Unmatched workbook positions are outside the
observed broker cohort, without a claim that other custody is closed or empty.

```sh
python scripts/compare_broker_observation.py captured.json export.xlsx \
  --contract-map private-contract-links.json \
  --usd-sar-rate 3.75 \
  --private private-comparison.json
```

The FX argument is an explicit research scenario with no default. Native USD
cash rows are used once; an aggregate `BASE` row is never added to them. The
comparison reads the latest `_Cash_Snapshot` entered balance without promoting
its date or note into account evidence.

Stdout contains only counts, booleans and fixed explanations. The optional
private report includes exact quantities, balances, order records, raw input
observations and SHA256 input hashes. It is written atomically with mode 0600;
invalid input preserves an existing report. Input paths, balances and broker
identifiers are excluded from errors and default output. The output cannot
replace or symlink to any input. Exit 0 means the comparison completed; exit 2
means invalid or unavailable input prevented it.

`execution_ready` and `funding_eligible` are always false. Offline origin is
not reauthenticated. Original retrieval time and broker source as-of remain
separate; missing account scope, timestamps, reservations, external custody
and fresh FX proof remain unknown. The report has no execution-certificate
`schema_version` and emits no `reconciliation_evidence`. It cannot populate
the native evidence property or pass the portfolio certifier.

Regression tests use fictional observations and generated XLSX packages.
They cover decimal precision, explicit/provisional links, cash comparisons,
unobserved custody, failed orders, different aggregate/detail vintages,
cross-currency arithmetic holdback, redaction, malformed/duplicate input,
private permissions and preservation of inputs/previous output.
