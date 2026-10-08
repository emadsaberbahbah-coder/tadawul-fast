# Portfolio input reconciliation

Portfolio actions 1.15.0 and opportunity allocation 1.25.0 require fresh,
explicit account-scoped evidence before issuing funding plans. Missing evidence
produces renderable research output with `execution_ready=false`. Portfolio
rows become `BLOCK`; funding, proposed proceeds and deltas are zero, NAV and
weights are unavailable, and order levels are blank. A failed check never
manufactures an empty custody account or preserves a newly trusted `HOLD`/`ADD`.

The evidence is a declared capture. The backend checks its consistency and
freshness; it does not independently authenticate broker origin. Authorized
capture processes must obtain the account data from its real custodian. Sheet
dates, copied balances and fingerprints cannot establish that origin.

## Required input

The pure API is
`certify_portfolio_inputs(holdings, evidence, fx_rates, *, now=None,
max_age_seconds=900, include_proposals=False)`. The 900-second bound covers
position, cash, capture and FX evidence; timestamps require seconds and a
timezone. All positions and cash belong to explicit account IDs. Coverage and
reservation flags must be boolean `true`, rather than truthy text or numbers.
Native currencies must be uppercase three-letter codes; ambiguous minor units
are rejected without conversion.

This example uses fictional identities and amounts:

```json
{
  "schema_version": 1,
  "source_ref": "synthetic://declared-account-export",
  "captured_at": "2026-10-09T09:00:00Z",
  "accounts": [{
    "account_id": "synthetic-account",
    "positions_asof": "2026-10-09T09:00:00Z",
    "positions_complete": true,
    "positions": [{"instrument_id": "synthetic-contract", "symbol": "SYNTH", "currency": "USD", "quantity": "10"}],
    "cash_asof": "2026-10-09T09:00:00Z",
    "cash_complete": true,
    "cash": [{"currency": "USD", "settled_cash": "200", "reserved_cash": "20", "reservations_complete": true}]
  }],
  "holding_links": [{"row_index": 0, "account_id": "synthetic-account", "instrument_id": "synthetic-contract", "position_symbol": "SYNTH", "symbol": "SYNTH.US", "currency": "USD"}],
  "funding_accounts": ["synthetic-account"],
  "fx_rates": [{"currency": "USD", "rate_to_sar": "3.75", "asof": "2026-10-09T09:00:00Z", "source_ref": "synthetic://fresh-FX-export"}]
}
```

`holding_links` supplies instrument linkage explicitly. The code does not guess
supplier/native aliases. Quantities must match exactly. Every positive position
in supplied accounts must appear in the holding cohort. An omitted instrument
is unknown even when a capture declares complete positions. Explicit zero and
partial quantities withhold funding and can produce private conditional review
proposals. Other custody requires its own complete position capture; unrelated
broker coverage cannot establish a zero external holding.

Funding uses only `settled_cash - reserved_cash`, converted by the supplied
fresh FX evidence. Reservations must cover all outstanding commitments once.
Base aggregates cannot substitute for native currency cash. Duplicate account,
instrument, link, cash currency or FX scopes are refused. FX must match the
caller's rate exactly; SAR is exactly one. Requested cash must match the
certified total to cents. Proposed sells and unsettled sale proceeds never fund
new tickets, including in advisory/target rebalance modes.

Current row-level quotes must also prove a successful acquisition and an actual
market as-of through the shared acquisition classifier. Retrieval time alone
cannot satisfy that check. Preserved, failed, unknown, future or stale quotes
withhold execution even when position/cash evidence is valid. Row-level FX
overrides cannot replace the certified rate. Existing policy thresholds, risk
gates and confirmation clocks are retained after these input checks.

## Transport and offline review

The portfolio route accepts top-level `reconciliation_evidence` privately.
The opportunity body carries it inside `portfolio`. The native Top10 request
reads document property `TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1` (maximum
9,000 characters) without modifying it. It forwards the supplied packet and
same-row quantity, currency, price, provider, warnings and retrieval stamp.
Missing/malformed properties supply no evidence; global script properties,
cash snapshots and paper controls are never substituted. The holding reader is
bounded to 500 holdings and 250 columns; incomplete reads, duplicate identities,
ambiguous proof headers and disabled holdings transport explicitly withhold
funding. A separate authorized capture workflow must populate that property.

The existing execution importer remains the append-only, trade-ID-scoped replay
for fills, reported commissions and conditional basis amendments. The new
offline command checks a position/cash packet without contacting either a
broker or workbook:

```sh
python scripts/tfb_reconcile_portfolio.py capture.json --now 2026-10-09T09:01:00Z --private-report private-review.json
```

The input object contains `holdings`, `reconciliation_evidence` and `fx_rates`.
Stdout contains only nonidentifying status/counts. The explicitly requested
private report contains cash totals and optional quantity proposals, with the
input hash and original-row fingerprint. It is written atomically with private
file permissions. `--now` supplies an explicit historical review clock; a report
is not a reusable execution certificate. Live callers validate the original
packet again using the current server clock. These proposals never close a ledger row, classify fees,
calculate realized P&L, credit settlement cash or send orders. Origin and fee
reconciliation remain unverified until authoritative evidence is supplied.

Native Top10 1.13.0 must be installed with its coordinated backend. Portfolio
decision native source is absent from this repository; its caller must forward
the same private evidence separately. Until then its backend safely renders
blocked research rows. No installation, live property/ledger/cash change,
broker ingestion credential or trade is performed by this patch. Safe rollback
is research-only operation; restoring unfunded legacy trust is unsafe.
