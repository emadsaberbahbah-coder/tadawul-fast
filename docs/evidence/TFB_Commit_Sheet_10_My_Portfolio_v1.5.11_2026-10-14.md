# TFB Commit Sheet - 10_My_Portfolio.gs v1.5.11 [PF SOURCE RECEIPT + EVIDENCE TRANSPORT]

Date: 2026-10-14.
- **Base:** origin/main `31d4675` (#774), plus commit `53c423d`, which mirrors the owner's export of the installed v1.5.10.
  - Received file: sha256 `c79af306…` (CRLF).
  - Mirrored file: sha256 `cb351aa6…` (LF, otherwise byte-identical).
- **Lane:** Apps Script (bound project). This is a paste of one file; no Render deploy and no workflow change.
- **Gates:**
  - Script Property `PD_SOURCE_RECEIPT` = off | observe | enforce. The default is off, which gives v1.5.10 output byte for byte.
  - The evidence transport is inert until the document property `TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1` exists. Nothing writes it today.

## Why

The 14 Oct full project review replayed the repository's real `finalize_decision_publication.run(publish=False)` offline on the workbook export (sync run 38486122061). Two Portfolio_Decision defects trace to this file.

**(A) PF_SOURCE_CONSUMPTION_UNPROVEN on every run.**
- `audit_decision_surface_freshness` 1.3.x requires Portfolio_Decision B2 to carry exactly one `source readiness VERIFIED (run <id>)` receipt.
- That run must equal the My_Portfolio `_Status` publication run.
- v1.5.10 never wrote a receipt, so final publication fails even when every other check passes.
- The producer was absent from the repository mirror, so the receipt could not be built until now.

**(B) Every holding renders `BLOCK` with "RECONCILIATION_REQUIRED: missing_evidence".**
- portfolio_actions 1.15+ certifies holdings only from `body.reconciliation_evidence`, and v1.5.10 never sent it.
- 16_Decision_Top10 already forwards the private document property `TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1`; Portfolio_Decision did not.
- Supplying the packet for one consumer did not reach the other.

## What changed

There are 7 anchored edits on the pinned base sha256 `cb351aa6…`, each asserted `count == 1` (`scripts/build_pf_receipt.py` in the review kit).

**Header and version.** `PD_VERSION` moves 1.5.10 → 1.5.11, with a WHY/WHAT block.

**Ten pure, guarded helpers.** All names are new, and none collide with the other three mirrored `.gs` files.

| Helper | Role |
| --- | --- |
| `pdReceiptMode_` | Reads `PD_SOURCE_RECEIPT`. Only `observe` or `enforce` (case and space tolerant) arm it; anything else is off. |
| `pdEvidencePacket_` | Same contract as Top_10's `dt10ReconciliationEvidence_`: string, at most 9,000 chars, JSON object, `schema_version === 1`. Otherwise null. Never logged or rendered. |
| `pdSrcReadinessCore_` | A one-page port of `dt10SourceReadinessCore_` (see the checks below). |
| `pdSrcStampMs_`, `pdSrcToken_`, `pdSrcCount_`, `pdSrcNormToken_` | Ports of the Top_10 parsers. |
| `pdSrcReadiness_` | Bounded `_Status` read of A1:J, at most 1,000 rows. It never writes. |
| `pdSrcCapture_` | Taken before the holdings read. |
| `pdReceiptNote_` | Builds the status-line fragment. |

`pdSrcReadinessCore_` requires all of the following:
- exactly one `My_Portfolio` row;
- one positive `run=` token;
- status OK, SUCCESS, VALID, PASS or COMPLETE;
- a parseable timestamp that is not in the future and is at most 8 h old (the final audit's My_Portfolio window);
- `acquired=N/N`, i.e. 100%;
- `acquisition=COMPLETE` and `data=COMPLETE`;
- a known `fetchfail_effective`;
- published rows equal to the holdings consumed;
- a row unchanged since the capture and no newer than the holdings read.

**`refreshPortfolioDecision`.**
- In observe or enforce mode, the My_Portfolio publication is captured before `pdCollectHoldings_`.
- After the render, the fragment is inserted right after the holdings note.
- The fragment requires the decision status to be `ok`.
- `observe` appends ` | [receipt-observe] would publish VERIFIED (run N)` (or `WITHHELD: why`). The final audit does not parse this text.
- `enforce` appends exactly one ` | source readiness VERIFIED (run N)` or ` | source readiness WITHHELD: why`.
- When the evidence property is present, the request body gains `reconciliation_evidence`.

**`pdSelfTest`.** It also reports:
- the receipt mode;
- the current My_Portfolio publication verdict;
- whether the evidence property is present. Its contents are never printed.

**Totals.** Top-level functions go from 79 to 89, with 0 removed. Smart quotes stay at 0. Non-ASCII characters go from 101 to 102: one em dash in the header comment, consistent with the file's existing headers. The source is ES5.

**Tests.**
- New `tests/js/pf_source_receipt.test.js`: the real source in a vm with Apps Script service stubs, synthetic holdings, accounts and runs.
- New `tests/test_pf_source_receipt.py`, collected by the whole-suite CI job. It runs that harness and feeds every produced status line to the real audit parsers, `_consumed_source_run` and `parse_surface_status`.
- New `tests/fixtures/pf_receipt_v1_5_10_golden.json`: the off-mode output of v1.5.10 on the synthetic scenario.
- `tests/js/apps_script_syntax.test.js`, already in CI, now compiles 4 sources.
- `ci.yml` is not touched, so this does not conflict with PR #773.

## Default OFF identity

With `PD_SOURCE_RECEIPT` unset or set to any value other than observe or enforce:
- `_Status` is never read;
- `receiptNote` is `''`;
- the B2 status line and the POST body equal v1.5.10's exactly.

Harness T01 proves this against the golden fixture, and live against a v1.5.10 copy when `PF_BASE_GAS` is given. It covers `''`, `on`, `true`, `1`, `OBSERVE-ish` and `yes`. With the evidence property absent, the request is unchanged (T05).

## Measured results

Node 22.22.0 and Python 3.11.17 with the pinned requirements.

```
PF_BASE_GAS=<v1.5.10> node tests/js/pf_source_receipt.test.js     (x3)
PASS 8/8 digest e4939fb5c5835798                                   (identical x3)
golden negative: PF_GAS=<v1.5.10> node tests/js/pf_source_receipt.test.js   -> T02 fails, exit 1
python -m pytest -q tests/test_pf_source_receipt.py                3 passed
golden negative (v1.5.10 installed as the shipped file)            3 errors
every tests/js/*.test.js + tests/test_dt10_calendar_completeness.js PASS
scripts/audit_repository_workflows.py                              errors=0 warnings=2 (pre-existing)
python -m pytest -q tests  (whole suite on this branch)             5057 passed, 33 skipped, 0 failed, 243 subtests (556.6 s)
```

The cases:

| Case | What it proves |
| --- | --- |
| T01 | off is byte-identical |
| T02 | observe tag; no declaration; POST unchanged |
| T03 | enforce writes exactly one receipt bound to the run |
| T04 | 21 withheld cases, each with exactly one declaration and a stated reason (listed below) |
| T05 | evidence transport: a valid packet is forwarded; 7 invalid forms are absent; the packet is never logged |
| T06 | the receipt and the evidence are independent |
| T07 | `_Status` clock parsing |
| T08 | self-test lines, never the packet |

The T04 withheld cases are:
- a stale, a future or a date-only stamp;
- run missing, repeated or non-numeric;
- 1/2 acquired or an invalid acquired receipt;
- acquisition or data not COMPLETE;
- fetchfail error or unknown;
- status FAILED;
- published and consumed row counts that differ;
- a repeated or missing row;
- no `_Status` sheet;
- decision status error;
- the row changed during the decision;
- a publication newer than the holdings read;
- a publication not ready before the read.

## Proof against the real final-publication audit (14 Oct export)

`counterfactual_pf_receipt.py` (review kit) runs the real v1.5.11 source on the export's real `_Status` rows at the moment the 14 Oct Portfolio_Decision refresh ran: 08:50:11 Riyadh, 4 holdings consumed, enforce. The native output is:

```
' | source readiness VERIFIED (run 38486122061)'   (capture ready, cohort 38486122061)
```

That fragment was written into Portfolio_Decision!B2 where v1.5.11 puts it. The unchanged audit was then replayed with its clock pinned to 09:40 Riyadh.

| Replay | Audit codes | Decision findings left |
| --- | --- | --- |
| R0 export + real PF receipt | [2, 2] | the 4 Top_10 findings only; PF_SOURCE_CONSUMPTION_UNPROVEN is gone |
| R1 + the cockpit v1.13.8 receipts | **[2, 0]** | none: the decision audit is clean |

R1 equals the review's hand-built scenario S2, so the S10 path ([0, 0] with D3 and the GM floor) now has a real producer for its PF receipt.

## Known limits

- **The receipt proves consumption only.** It shows this decision read the My_Portfolio publication of run N, within 8 h and unchanged. It does not authenticate custody. The BLOCK rows remain until a real reconciliation packet exists (decision D4).
- **The 8 h window.** A manual refresh more than 8 h after the My_Portfolio publication writes `WITHHELD: publication stale (>8h)`. That is correct, and matches the final audit.
- **Timing of the production job.** The 03:47 job still runs before the 08:50 cockpit refresh, so it sees the previous day's receipt. The release step (P0-1, dispatched after the cockpit refresh) is what pairs a fresh receipt with a final audit.
- **What the row check catches.** "Published rows == holdings consumed" compares the My_Portfolio rows this decision read with the rows the sync published. A manual edit to the page after the sync withholds the receipt. It does not detect a holding that exists only in the ledger.
- **Partial mirror.** The repository holds 4 of about 23 bound files. Install by replacing this one file in the Apps Script editor. Never run a full `clasp push` from this partial mirror.

## Operator steps

1. **Merge the PR.** Nothing runs from GitHub; this keeps the mirror and the tests.
2. **Install.** Open the workbook, then Extensions → Apps Script → `10_My_Portfolio.gs`. Replace that file's whole content with v1.5.11 and save. Touch no other file.
3. **Self-test.** Run `pdSelfTest`. It should show:
   - `source receipt mode (PD_SOURCE_RECEIPT): off`
   - `My_Portfolio publication: ready, run <today's run>` (before 10:46 Riyadh)
   - `reconciliation evidence property: absent`
4. **Arm observe.** In Project Settings → Script Properties, add `PD_SOURCE_RECEIPT` = `observe`. Run Refresh Portfolio Decision. B2 should then show `| [receipt-observe] would publish VERIFIED (run N)`.
5. **Arm enforce.** After one observe read-back, set `PD_SOURCE_RECEIPT` = `enforce`. On the next refresh, B2 should show exactly one `| source readiness VERIFIED (run N)`.

## Rollback

Delete the `PD_SOURCE_RECEIPT` Script Property. The page then writes v1.5.10 output. To remove the code, paste v1.5.10 (commit `53c423d`) back. Nothing else depends on it.

## Owner decisions

- **D-PF1:** arm the receipt (observe, then enforce). Recommended now.
- **D4 (open):** the evidence policy and its supplier. This build only provides the transport.
