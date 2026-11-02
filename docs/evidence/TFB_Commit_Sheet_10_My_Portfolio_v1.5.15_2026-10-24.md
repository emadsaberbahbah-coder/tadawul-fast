# TFB Commit Sheet - 10_My_Portfolio.gs v1.5.15 [PD-ALLCASH: A BROKER-PROVEN ALL-CASH BOOK IS A DECIDED STATE]

Date: 2026-10-24.
- **Base:** v1.5.14 (`ca2f87c7`, sha256 `98e9518600220a186a02d32d85d06f68d96fd6882e64645a7290a40cb273dddd`, 4,544 lines).
- **Result:** sha256 `343ade4d268023d0b94162ba519acc275885e9bef4ec8fc741cac95b1164bd34`, 4,636 lines (+92).
- **Lane:** Apps Script (bound project). A paste of one file. It supersedes v1.5.14 for installation. Nothing in this PR deploys or touches the live project.
- **Gate:** Script Property `PD_ALLCASH_OK` = `enforce` (default, owner decision 2026-10-24) | `off` (every line byte-identical to v1.5.14). Any other value is `enforce`.

## Why

Verified on the live workbook today (24 Oct). The brokerage account has been all cash since the DDI sale on 22 Oct (0 positions). The only My_Portfolio row, 5023.SR, is outside the execution scope (`PD_EXECUTION_EXCLUDE`), so the request carries 0 holdings and the backend answers `status: empty` (`core/analysis/portfolio_actions._build_verified`, "no holdings rows supplied"). In `pdReceiptNote_` the rule `if (src.ready && st !== 'ok') src = {ready:false, reason:'decision status ' + st}` then withholds the receipt. Portfolio_Decision B2 read:

```
Last run 2026-10-24 17:43:37 | status: empty | holdings 1 rows from My_Portfolio (qty/cost from cost-basis: 1) | source readiness WITHHELD: decision status empty | ibkr evidence SENT (capture 17:39 age 4m; linked 0/0; cash 82819.20 SAR; USD 3.75; excluded 5023.SR) | ...
```

The release gate (`scripts/audit_decision_surface_freshness.py`) needs the embedded status token to be `OK` (`PF_STATUS_NOT_OK`) and exactly one `source readiness VERIFIED (run N)` receipt (`PF_SOURCE_CONSUMPTION_UNPROVEN`). Both failed on every attempt since 23 Oct, so no Top-10 ticket could be sized. The broker capture proves the book is empty; that is a decided state, not a failure, and it must not block the feed.

## What changed

Ten anchored edits on the pinned base, each asserted `count == 1` (build script in the review kit; the table below gives the base and result line numbers).

| Edit | Base line | v1.5.15 line | Lines |
| --- | --- | --- | --- |
| E1 header `Version: 1.5.15` and this dated WHY block | 4 | 4 | +36 |
| E2 `PD_VERSION = '1.5.15'` | 2570 | 2606 | 0 |
| E3a `pdReceiptNote_` gains an optional 7th argument `allCash` | 3074 | 3110 | 0 |
| E3b the status rule accepts `'empty'` only when `allCash === true` | 3081 | 3117 | +3 |
| E4 new `PD_ALLCASH_PROP`, `PD_ALLCASH_TOKEN`, `pdAllCashMode_`, `pdAllCashProof_` | 3099 | 3138 | +39 |
| E5c `pdIbkrApply_` doc comment names the new result fields | 3554 | 3632 | +1 |
| E5a `pdIbkrApply_` result object carries `positions`, `linked`, `ageMs` (null) | 3556 | 3635 | +1 |
| E5b ... filled from the plan when the capture is valid (read-only) | 3566 | 3646 | +5 |
| E6 `refreshPortfolioDecision` computes the gate, passes it to the receipt, writes the token and `ok` | 4127 | 4212 | +5 |
| E7 `pdSelfTest` reports `all-cash gate (PD_ALLCASH_OK): enforce|off` | 4517 | 4607 | +2 |

**The proof** (`pdAllCashProof_(payload, ibkr)`, pure, fail-closed) is true only when all of these hold; any missing or unknown value returns false:
- `String(payload.status).toLowerCase() === 'empty'` and `payload.actions` is an array of length 0 (the "decisions 0" in the token is checked, not assumed);
- `ibkr.sent === true` (pdIbkrApply_ sets it only in enforce with a valid capture whose age is in `[0, PD_IBKR_SEND_MAX_AGE_MS]`), `ibkr.note` contains `ibkr evidence SENT`, and `ibkr.ageMs` is a finite number inside that same window;
- `ibkr.positions === 0` and `ibkr.linked === 0` (the plan's counts; `positions` counts capture lines with quantity > 0, exactly as the self-test prints them).

The v1.5.14 result object of `pdIbkrApply_` was `{note, excluded, controls, sent}`; it carried no counts. Rather than infer them from the display note, E5a/E5b expose the plan's `positions`, `linked` and `ageMs` on the result (null when there is no valid capture). The broker reads, the exclusion logic, the body mutation and the note are untouched.

**When the proof holds and the property is `enforce`:**
- `pdReceiptNote_` treats `'empty'` as an acceptable decision status. The source cohort, the publication freshness, the consumed-rows check and the v1.5.13 binding are unchanged and still withhold on their own.
- The B2 status token is `ok` and the line gains one token right after it: ` | book all-cash (broker 0 positions, decisions 0)`.
- The Logger line (`done status=empty`), the return value (`'empty'`), the request body, the render and the action log are unchanged.

**Otherwise** (property `off`, no capture, stale capture, positions > 0, any other status, actions present, IBKR mode observe or off) every line, the request and the render are v1.5.14 byte-identical (T3/T4 below prove it against the golden and against the live base).

New B2 (synthetic 24 Oct shape, the harness):

```
Last run 2026-10-24 17:43:37 | status: ok | book all-cash (broker 0 positions, decisions 0) | holdings 1 rows from My_Portfolio (qty/cost from cost-basis: 0) | source readiness VERIFIED (run 6363636363) | ibkr evidence SENT (capture 17:39 age 4m; linked 0/0; cash 82819.20 SAR; USD 3.75; excluded 9999.SR) | route v4.18.2 | actions v1.16.0 | builder v1.26.1 | req req-synthetic | 0s | ActLog: off
```

**Totals.** Top-level functions go from 118 to 120: 2 added (`pdAllCashMode_`, `pdAllCashProof_`), 0 removed, 4 changed (`pdReceiptNote_`, `pdIbkrApply_`, `refreshPortfolioDecision`, `pdSelfTest`). Two vars added. ES5 only, added lines ASCII; the non-ASCII line count stays at 97.

## Risk table

| Risk | Mitigation | Residual |
| --- | --- | --- |
| The gate marks a non-empty book `ok` | The proof needs the backend's `empty` with 0 actions AND the broker capture of this run with 0 positions and 0 links; 9 fail-closed variants (T4) and 26 unit inputs (T5) are byte-identical to v1.5.14 | A capture that omits positions is rejected by `pdIbkrPlan_` ('positions read missing') and never sent |
| The receipt is published for a stale or foreign publication | Only the status rule changed; cohort, freshness, consumed-rows and binding checks are untouched (T7 shows the cohort check still withholds under `allCash`) | none known |
| A zero-quantity closed line in the capture | Counts as 0 positions (the v1.5.12 definition); the packet still carries the line, so the backend sees it | consistent with the self-test's `positions N` |
| The new token confuses a parser | `RUN_RE` reads `status: ok`; the token has no `/` so `POOL_RE` ignores it; `_consumed_source_run` counts `source readiness` only | none; proven on the real parsers and `audit_surfaces` |
| Operator wants the old behaviour | `PD_ALLCASH_OK=off` is byte-identical to v1.5.14 (T3) | none |
| `pdIbkrApply_` result shape | Three fields added, null by default; no caller reads them except the proof | none |

## Measured results

```
PF_BASE_GAS=<v1.5.14> node tests/js/pf_allcash_ok.test.js     (x3)  PASS 7/7 digest 6dbbc21bc88b5d26
PF_BASE_GAS=<v1.5.14> node tests/js/pf_source_receipt.test.js (x3)  PASS 8/8 digest 9cc07d4571cda7ad   (unchanged from v1.5.14)
PF_BASE_GAS=<v1.5.14> node tests/js/pf_ibkr_evidence.test.js  (x3)  PASS 14/14 digest c1ad80b284150174 (differs from v1.5.14 only in the dumped self-test: version and the new gate line)
node tests/js/apps_script_syntax.test.js                               5 sources compiled
golden negative: PF_GAS=<v1.5.14> node tests/js/pf_allcash_ok.test.js -> T1 passes, T2 fails ('status: empty ... WITHHELD: decision status empty'), exit 1
mutations (proof ignores positions / sent / age / status; default off; receipt ungated; token omitted; status token kept; kill switch ignored): 9/9 caught (T2, T3, T4 or T5)
python -m pytest -q tests/test_pf_allcash_ok.py tests/test_pf_source_receipt.py tests/test_pf_ibkr_evidence.py   (x3)  20 passed
```

Audit check (the real `scripts/audit_decision_surface_freshness` on the harness lines, `expected_source_run=6363636363`, `_Status` run 6363636363):

```
v1.5.15 enforce : parse_surface_status -> (2026-10-24 17:43:37, 'OK');  _consumed_source_run -> 6363636363;  audit_surfaces -> exit 0, findings []
v1.5.14 / off   : parse_surface_status -> (2026-10-24 17:43:37, 'EMPTY'); _consumed_source_run -> None;        audit_surfaces -> exit 2, ['PF_SOURCE_CONSUMPTION_UNPROVEN', 'PF_STATUS_NOT_OK']
```

### Tests

- New `tests/js/pf_allcash_ok.test.js`: the real source in a vm on the 24 Oct shape (one sukuk row outside scope, a capture with 0 positions and USD cash 22085.12 = 82819.20 SAR, the backend's `empty` skeleton). T1 golden negative (v1.5.14 writes `status: empty` and withholds); T2 enforce writes the new line with the receipt, the request/render/log unchanged, for 8 property spellings, a zero-quantity line, receipt off/observe and an upper-case status; T3 `off` deep-equals the golden for the main scenario and all 9 variants; T4 fail-closed variants (no capture, stale 24m, broker 1 position, status ok with 0 rows, status error, empty with actions, ibkr observe, ibkr off, folder unset) are v1.5.14 byte-identical; T5 26 bad inputs to the pure proof and 8 property values; T6 the result fields of `pdIbkrApply_`; T7 self-test line and the 6-argument receipt call unchanged.
- New `tests/fixtures/pf_allcash_v1_5_14_golden.json`: generated from the v1.5.14 source (sha256 pinned) and holds its lines, requests, render digests, every top-level function name (118) and the non-ASCII line count (97).
- New `tests/test_pf_allcash_ok.py` (collected by the whole-suite CI job like its siblings): feeds every line to the real parsers and to `audit_surfaces` with the strict run; three identical harness runs; the live base from git must reproduce the golden and fail the harness; version lockstep; function inventory (0 removed, exactly 2 added); added lines ES5 and ASCII (git-diff based; skipped only when the base is unreachable).
- `tests/test_pf_source_receipt.py` and `tests/test_pf_ibkr_evidence.py` pass unchanged; neither asserted the old empty-book behaviour.

## Install (OPERATOR STEPS) and rollback

1. Open the bound Apps Script project and the file `10_My_Portfolio.gs`; confirm the editor shows `Version: 1.5.14` and `PD_VERSION = '1.5.14'`.
2. Replace the whole file with `apps_script/10_My_Portfolio.gs` from this commit (sha256 above). Save.
3. Run `pdSelfTest` from the editor. Expect `[MP-DEC v1.5.15] self-test` and the line `all-cash gate (PD_ALLCASH_OK): enforce` (no Script Property is needed for the default).
4. Run `refreshPortfolioDecision` once (or wait for the capture watcher). With a fresh capture at 0 positions, B2 reads `status: ok | book all-cash (broker 0 positions, decisions 0) | ... | source readiness VERIFIED (run N) | ibkr evidence SENT (...)`. Without a fresh capture the line is the v1.5.14 line.
5. Rollback: set Script Property `PD_ALLCASH_OK` to `off` (byte-identical v1.5.14 lines), or paste v1.5.14 again.
