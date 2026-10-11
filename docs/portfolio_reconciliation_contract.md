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

## Operator-maintained cash export template (v1.0.0)

`_Cash_Snapshot` is an operator-maintained export-audit record. The backend and
native Top10 do not read it for funding. The export audit certifies only the
last appended cash record; it neither aggregates accounts nor falls back to
an older valid row. Its default cash/FX age bound is 24 hours. This audit check
is separate from the backend's 900-second account evidence requirement.

Keep the seven v1.0.1 columns in their existing order and append the evidence
columns; existing cash histories need not be rewritten:

| Existing columns | Appended for every account | Also appended for non-SAR accounts |
| --- | --- | --- |
| Date, Time, Type, Amount SAR, Balance SAR, Delta vs Prev, Note | Account, Currency, Balance Type | Balance, FX to SAR, FX As Of |

The audit accepts exactly these header names and aliases:

| Field | Header names | Requirement |
| --- | --- | --- |
| Account scope | `Account`, `Account ID` | Nonblank explicit identity |
| Native currency | `Currency`, `Ccy` | Three-letter currency |
| Balance semantics | `Balance Type` | `settled_cash` (spaces are normalized to underscores) |
| Full timestamp | `As Of`, `Timestamp` | Optional replacement for Date + Time; a populated invalid alias fails closed |
| Legacy timestamp | `Date`, `Time` | Actual date and Time; explicit `00:00` is valid and a blank Time fails |
| Native settled balance | `Balance`, `Native Balance` | Required for non-SAR; absent SAR native-balance headers use `Balance SAR` |
| Recorded converted balance | `Balance SAR` | If populated, must equal native Balance × FX within 0.01 SAR |
| Conversion rate | `FX to SAR` | Positive, explicit for non-SAR; SAR is exactly one |
| FX vintage | `FX As Of` | Full timestamp for non-SAR, no later than the cash timestamp |

`Type=SNAPSHOT`, note text, `Amount SAR` and `Delta vs Prev` do not establish
settled cash. A custodian's “Available to Trade” figure is not accepted as
`settled_cash` under the current contract. Settled and reserved amounts must be
obtained separately for backend evidence; available funding remains settled
minus reserved, with complete outstanding commitments captured once. The
choice of per-account or consolidated export rows remains an owner decision;
the current audit certifies one latest row and cannot certify a consolidated
custody capture from that row alone.

## Offline custodian capture builder (v1.0.0)

`scripts/tfb_capture_evidence.py` writes only a validated schema-version 1
evidence packet to an explicitly chosen local file. It calls the same pure
certifier as `tfb_reconcile_portfolio.py`, keeps the 900-second bound, prints
only nonidentifying status/counts, and atomically writes mode `0600`. Invalid
captures leave an existing destination untouched. It contacts neither a broker
nor a workbook and does not authenticate the declared source.

For JSON, supply the existing offline review object: `holdings`,
`reconciliation_evidence` (the exact packet shown above), and caller `fx_rates`
(a currency-to-rate object). Explicit quantities, account coverage, instrument
links, native settled cash, reserved cash and reservation completeness remain
mandatory. The tool does not derive these facts from generic balances.

```sh
python scripts/tfb_capture_evidence.py custodian-capture.json \
  --now 2026-10-09T09:01:00Z --max-age-check --output private-evidence.json
```

`--now` requires seconds and a timezone. The regular certification checks all
positions, capture and FX timestamps, plus funding-account cash; the additional
`--max-age-check` checks *every* account's declared cash timestamp too. Any
timestamp older than 900 seconds, in the future or after capture fails. Exactly
900 seconds is accepted. The historical clock is for review only: live callers
must certify the original evidence again using the current server clock.

For CSV, use a `record_type` column plus the union of the fields below. Blank
unused cells are allowed; nonblank fields outside a record's listed fields,
unknown/duplicate headers, missing fields and extra cells fail. One `capture`
record is required. Records may appear in any order; `holding.row_index` must
cover contiguous zero-based rows, matching My_Portfolio order. Booleans must
be lowercase `true` or `false`; quantities, cash and rates retain their exact
decimal strings. Explicit `caller_fx` records keep caller rates separate from
FX evidence. SAR needs no market FX proof.

| record_type | Required nonblank fields |
| --- | --- |
| `capture` | source_ref, captured_at |
| `account` | account_id, positions_asof, positions_complete, cash_asof, cash_complete |
| `position` | account_id, instrument_id, symbol, currency, quantity |
| `cash` | account_id, currency, settled_cash, reserved_cash, reservations_complete |
| `holding` | row_index, symbol, currency, quantity |
| `link` | row_index, account_id, instrument_id, position_symbol, symbol, currency |
| `funding` | account_id |
| `fx` | currency, rate_to_sar, asof, source_ref |
| `caller_fx` | currency, rate_to_sar |

Replace the input extension with `.csv` or pass `--format csv` to use that
mapping. Operator captures must originate at the real custodians; the tool
supplies no credentials, origin witnesses, missing balances or timestamps.
After private review, an authorized operator may populate document property
`TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1` (maximum 9,000 characters) and arrange
the separate Portfolio_Decision caller to forward it. `packet_bytes` in stdout
is a size count for review; saving a packet does not install it or certify a
later decision run. A packet above the property limit cannot be forwarded by
the native Top10 transport.
