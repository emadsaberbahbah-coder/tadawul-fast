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

## Research view (portfolio actions 1.16.0, default off)

Added 2026-10-13 (P0-6, review blocker 4). Nothing in this section changes the
certifier, its 900-second bound, funding or any rule above. With the gate off
the portfolio page is exactly the 1.15.1 output apart from the version string.

### Transport correction

Two separate callers carry evidence, and only one of them reads the document
property:

- The Top10 cockpit (`apps_script/16_Decision_Top10.gs`) reads document
  property `TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1` and forwards it inside
  the opportunity body. That property feeds only the Top10 board.
- Portfolio_Decision is produced by `POST /sheet-rows/portfolio-actions`. The
  route reads evidence only from `body.reconciliation_evidence`, sent by its
  native Apps Script caller. That caller is not in this repository and does
  not read the document property. Writing the property therefore cannot
  change Portfolio_Decision; its caller must forward the packet itself.

A live reason of `missing_evidence` on Portfolio_Decision means the request
carried no packet at all. It does not show whether the property is populated.
A packet older than 900 seconds is reported as `invalid_evidence`, not as a
stale status, because the certifier rejects the whole packet.

### What the gate does

`TFB_PF_RESEARCH_VIEW` is read on every call. Accepted values are `off`,
`observe` and `enforce`, in any letter case. Since portfolio_actions 1.17.0
(2026-10-15, owner decision) an unset or blank value reads `enforce`; any
other value is treated as `off`. To turn the view off, set the explicit value
`off` on Render. Unsetting it no longer turns it off.

When funding is not eligible, each holding gets a tier from the 900-second
certification:

| Certifier row result | Tier | Tag | Row shown |
| --- | --- | --- | --- |
| trusted, but funding withheld (cash, quote, FX, truncated holdings) | matched | `[custody-matched]` | research row |
| `missing_evidence`, `invalid_evidence`, `custody_unknown`, `stale_positions`, `future_positions`, `invalid_positions_time`, `positions_incomplete` | unverified | `[custody-unverified]` | research row |
| `quantity_mismatch`, `position_closed`, `position_missing`, `position_identity_mismatch`, `inactive_zero`, or any other status | contradicted | none | the 1.15.1 blank row, unchanged |

For a research row the engine computes the rule verdict with `decide_action`
(weights unavailable, so no cap trim), then the de-minimis, reduce-policy and
drawdown seams. It never runs the ADD confirmation clock, its session shadow,
the ADD-loser veto or the F-1a tag, so persisted confirmation state is never
read or advanced from an unreconciled run. An ADD verdict is never named or
sized. The row says "new-money verdict withheld until fresh positions and
settled cash are reconciled".

- `observe`: every number, KPI and alert is identical to `off`. The reason and
  advisor note gain ` | [research-observe] <tag> research verdict <X>:
  <reason>`. `detail.research_view` and `meta.research_view` are added.
- `enforce`: the action stays `BLOCK` and the reason keeps the prefix
  `RECONCILIATION_REQUIRED: <status>`, followed by ` | <tag> research verdict
  EXIT|TRIM|HOLD|BLOCK: <reason>; not executable`. Market value, cost, P&L,
  P&L % and stop / TP1 / TP2 are shown. A sukuk row keeps a blank ladder.
  When the verdict is a cost-basis BLOCK, that row's cost, P&L and P&L % stay
  blank.

### What stays withheld in every mode

Proceeds, suggested deltas, `funds_from`, weights and sector weights stay zero
or blank, and `detail.execution_ready` and `meta.execution_ready` stay false.
The funding KPIs (`deployable_sar`, `adds_funded_sar`, `proceeds_pending_sar`,
`capital_unallocated_sar`) stay 0. `portfolio_value_sar`, `cash_sar` and
`cash_pct` stay blank. `sector_summary` stays empty. The single
`portfolio_inputs_unverified` alert and `meta.input_certification` are
unchanged. No account ID, source reference, cash or reservation figure from
the packet is ever emitted.

Under `enforce` the holdings KPIs follow the F3/F7 rules.
`holdings_value_sar` is filled only when no row is contradicted, the holdings
input is complete, and every research row has a positive market value.
`cost_basis_sar`, `pnl_sar` and `pnl_pct` use only rows that carry both cost
and P&L. They are withheld when any row is contradicted or the holdings input
is incomplete. `meta.research_view.kpi_basis` is
`sheet_quantities_custody_unverified` when any of them is filled and
`withheld` otherwise. `kpi_withheld` names the reasons. These totals come from
sheet quantities whose custody is not proven, so they are not a reconciled
book value.

Two more reasons withhold all four holdings KPIs. Each one shows that a total
is wrong, not only that it is unproven:

- `unrepresented_positions`: the packet shows a positive position that no
  sheet row links to, so any sheet total is short. The rows keep their own
  figures.
- `holding_fx_mismatch`: a holding's FX differs from the caller's rate. Every
  research row goes through the FX rule of the funded path (SAR is exactly 1;
  any other currency must equal the caller rate, row FX columns included).
  This applies even when the certifier stopped before reaching its own FX
  check. A rejected row shows none of its seven SAR display fields, and its
  reason gains `SAR figures withheld: holding FX differs from the supplied
  rate`. `detail.research_view.fx_rejected` and
  `meta.research_view.fx_rejected_rows` record the result.

`holding_quote_unverified` and `fx_unverified` mean an input is unproven, not
contradicted. In this build they do not withhold the totals; that is an open
owner decision.

If the research view itself fails, the page falls back to the `off` output
and `meta.research_view.status` reads `error`.

### Deployed readback lockstep

`scripts/verify_core_repair_readback.py` 1.2.0 accepts research amounts only
when the run declares the mode with `--expect-pf-research-view`. The workflow
passes the value of the Actions Variable `TFB_PF_RESEARCH_VIEW_EXPECT`.
Since readback 1.2.2 (2026-10-15) both the workflow fallback and the CLI
default are `enforce`, in lockstep with portfolio_actions 1.17.0. With `off`
the probe is unchanged, and an enforce page fails with `uncertified portfolio
amount was published: holdings_value_sar`.
Under a declared `enforce` every money, execution, weight, sector, cash KPI
and alert check is kept. The served page must also declare `enforce`, carry
`[custody-unverified]`, end with `; not executable` and name no ADD verdict. A
mode the page declares about itself is never enough.

Arming order (historical, 1.16.0): set `observe` on Render first (the
unchanged readback stays green). Set `enforce` only after readback 1.2.0 is on
main, and set `TFB_PF_RESEARCH_VIEW_EXPECT` to the same word at the same time.

Rollback (1.17.0 and later): set `TFB_PF_RESEARCH_VIEW=off` on Render AND the
Actions Variable `TFB_PF_RESEARCH_VIEW_EXPECT=off` together. Unsetting either
one leaves it at the `enforce` default.

### Not provided by this build

The research view does not supply evidence. A holding becomes executable only
through a valid 900-second packet forwarded by the Portfolio_Decision caller.
A longer daily custody tier, which could let HOLD, TRIM or EXIT act on
evidence up to 24 hours old, is a separate owner decision (D4). It is not
built here.
