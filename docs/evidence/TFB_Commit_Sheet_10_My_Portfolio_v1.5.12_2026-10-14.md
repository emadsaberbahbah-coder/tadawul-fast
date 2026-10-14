# TFB Commit Sheet - 10_My_Portfolio.gs v1.5.12 [IBKR CAPTURE EVIDENCE + EXECUTION SCOPE]

Date: 2026-10-14.
- **Base:** PR #776 head `7c8abdc` (v1.5.11, sha256 `358d661a...`). Result sha256 `7161b974...`, 4,413 lines.
- **Lane:** Apps Script (bound project). A paste of one file; no Render deploy and no workflow change.
- **Gate:** Script Property `PD_IBKR_EVIDENCE` = off | observe | enforce. The default is off, which gives v1.5.11 output byte for byte and never touches Drive.
- **Supplier:** a daily read-only scheduled task (08:40 Riyadh) writes one capture file to a private Drive folder. It is configured outside the repository; nothing here calls IBKR.

## Why

v1.5.11 gave Portfolio_Decision its receipt and an evidence transport, but every holding still renders BLOCK: nothing supplies the evidence, and the request could not be certified even with it.

On 14 Oct the real `portfolio_actions` build was replayed offline on the owner's read-only IBKR positions, balances and working orders. It withheld on four separate causes, in this order:

1. **custody_unknown.** The 5023.SR sukuk is held outside the IBKR account, so no capture can link it, and one unlinked holding withholds the whole portfolio.
2. **fx_basis_mismatch and cash_request_mismatch.**
   - The caller FX comes from TFB_FX_LOOKUP (USD 3.75547), while evidence proofs must equal the caller FX exactly.
   - The panel cash must equal the certified cash at the cent, half-even.
3. **holding_quote_unverified.** This page never sent the quote provenance columns: Data Provider, the Warnings acquisition tokens, and Last Updated (UTC).
4. **Freshness.** The evidence must reach the request within 900 s of the broker read (`DEFAULT_MAX_AGE_SECONDS`).

With the sukuk outside the execution scope, USD at the 3.75 peg in both places, the provenance columns sent and a fresh capture attached, the same build returned `certified` and `funding_eligible`.

## What changed

There are 18 anchored edits on the pinned base, each asserted `count == 1` (`scripts/build_pf_ibkr.py` in the review kit).

**Modes.** Any value of `PD_IBKR_EVIDENCE` other than observe or enforce means off.

| Mode | Behaviour |
| --- | --- |
| off | No Drive read. Request, render and status line are v1.5.11 byte-identical. |
| observe | Reads the newest capture and appends one ` \| [ibkr-observe] would send (...)` or ` \| [ibkr-observe] would not send: why`. The request is unchanged. |
| enforce | Applies (a) to (d) below and appends ` \| ibkr evidence SENT (...)` or ` \| ibkr evidence NOT SENT: why`. |

**What enforce does.**
- **(a) Execution scope.** Rows whose Symbol is in `PD_EXECUTION_EXCLUDE` (a comma list, for example `5023.SR`) are not sent. They render in a new OUTSIDE EXECUTION SCOPE section, never weighted, funded or actioned. They still count as consumed for the v1.5.11 receipt, so `published rows == holdings consumed` keeps holding.
- **(b) FX.** `fx_rates.USD = 3.75` (the SAMA peg). Other currencies are untouched.
- **(c) Quote provenance.** `Data Provider`, `Warnings` and `Last Updated (UTC)` travel with each holding, through an optional `extraFields` argument to `pdCollectHoldings_`.
- **(d) Evidence.** The IBKR capture is the only evidence source, so the v1.5.11 document-property packet is dropped. When the newest capture is at most 14 minutes old, the evidence is built and the request cash is replaced; otherwise no evidence is sent and the panel cash is kept.

**The evidence packet** is `body.reconciliation_evidence`, schema_version 1:
- one account, with complete positions;
- settled cash less working BUY limit reservations;
- explicit links from `SYMBOL.US` + USD to the IBKR contract id;
- one USD proof at `3.75` with source `policy://sama-usd-peg-3.75`.

**The request cash** becomes the certified cash, computed in exact integers: `(settled - reserved) x 375`, rounded half-even to the cent. That is how `portfolio_actions` compares it.

**Capture contract.**
- A capture is a JSON file named `TFB_IBKR_Capture_*.json` in `PD_IBKR_CAPTURE_FOLDER_ID`, with schema `tfb.ibkr-capture/v1`.
- It holds the verbatim positions, balances and orders responses, plus `captured_at` with an explicit offset.
- It is rejected with a stated reason when:
  - the schema or timestamp is wrong;
  - `captured_at` is more than 2 minutes in the future;
  - `captured_at` and the Drive creation time differ by more than 10 minutes;
  - a read is missing;
  - a contract id repeats;
  - a quantity is negative or unreadable;
  - cash is in a currency with no proof;
  - settled cash is negative;
  - there is no cash row.

**Reservations.**
- Orders carry no currency, so a working BUY limit order is reserved against the only cash currency, rounded up to the cent.
- A BUY without a limit price, an unknown side, or several cash currencies declares `reservations_complete: false`. The packet is still sent, the backend withholds cash, and the panel cash is kept.

**Watcher.**
- `pdIbkrCaptureWatch`, a time-driven trigger every 5 minutes installed by `pdInstallCaptureWatcher`, acts only in observe or enforce, and only inside `PD_IBKR_WATCH_WINDOW` (default 08:30-10:30 Riyadh; `always` is allowed).
- It runs `refreshPortfolioDecision` once per new capture that is at most 10 minutes old.
- It records `PD_IBKR_LAST_PROCESSED` only after a completed refresh, so a network or HTTP failure is retried on the next tick while the capture is fresh.
- It holds the script lock and skips when the lock is busy.
- `pdRemoveCaptureWatcher` deletes only its own triggers.

**`refreshPortfolioDecision`** now returns its outcome: the payload status, `NETWORK ERROR` or `HTTP <code>`. The action log receives the effective controls.

**`pdSelfTest`** also reports:
- the mode and the exclusions;
- the newest capture's verdict (valid or rejected, age, positions, linked x/y, whether the cash is certifiable);
- the watcher state.

It never prints amounts or the packet. The packet is never logged or rendered.

**Totals.**
- Top-level functions go from 89 to 116: 27 added, 0 removed, 4 changed (`pdCollectHoldings_`, `refreshPortfolioDecision`, `pdRenderPayload_`, `pdSelfTest`).
- No top-level var is removed.
- The source parses as ES5 with acorn (ecmaVersion 5).
- Added lines are ASCII; the non-ASCII count stays at 102 and smart quotes at 0.
- No name collides with the other mirrored `.gs` files.

**New permission.** This adds DriveApp to the project. Run `pdSelfTest` once from the editor right after pasting and approve the Drive permission. Until then, time-driven triggers in this project may fail authorization.

## Default OFF identity

With `PD_IBKR_EVIDENCE` unset or set to any value other than observe or enforce, Drive, the lock service and triggers are never touched. The B2 line, the POST body and every sheet write equal v1.5.11's exactly.

Harness P01 proves this against `tests/fixtures/pf_ibkr_v1_5_11_golden.json` (generated from v1.5.11), and live against a v1.5.11 copy when `PF_BASE_GAS` is given. It covers unset, `''`, `on`, `true`, `1`, `ENFORCE-ish` and `yes`. The v1.5.11 receipt harness gives the same digest on v1.5.11 and v1.5.12.

## Measured results

Node 22.22.0 and Python 3.11.17 with the pinned requirements.

```
PF_BASE_GAS=<v1.5.11> node tests/js/pf_ibkr_evidence.test.js   (x3)  PASS 14/14 digest d113f251da0ce6d5
node tests/js/pf_source_receipt.test.js                         (x3)  PASS 8/8 digest e4939fb5c5835798 (same as on v1.5.11)
mutation negatives (8 single-line breaks of the new code)              8/8 caught, exit 1
python -m pytest -q tests/test_pf_ibkr_evidence.py tests/test_pf_source_receipt.py   11 passed (x3)
node tests/js/apps_script_syntax.test.js                               4 sources compiled
python -m pytest -q tests  (whole suite on this branch)                5065 passed, 33 skipped, 0 failed
trial merge onto origin/main 030e22e                                   clean; whole suite 5112 passed, 33 skipped, 0 failed
```

The mutation negatives each break one thing:
- half-even becomes half-up;
- the exclusion is ignored;
- the USD peg is not applied;
- stale evidence is sent;
- provenance is not sent;
- the reservation rounds down;
- the watcher marks before refreshing;
- off reads Drive.

| Case | What it proves |
| --- | --- |
| P01 | off is byte-identical (request, render, line); no Drive, lock or trigger use |
| P02 | observe: one tag, unchanged request and render, the exact Drive query; the stale variant |
| P03 | enforce + fresh capture: 3 rows sent, provenance added, `USD 3.75`, cash replaced by the certified cents, exact packet, out-of-scope title and row rendered, nothing private logged |
| P04 | the v1.5.11 receipt still binds with an excluded row (consumed 4 = published 4) |
| P05 | enforce + stale capture: scope, peg and provenance only; no evidence; the document-property packet is dropped |
| P06 | 19 rejected captures or reads, each with its reason and no evidence |
| P07 | BUY reservations: ceiling cents (236.22 on two orders); market buy, unknown side, over-reserved and multi-currency withhold the cash and keep the panel |
| P08 | links: USD stock only, `BRK B` becomes `BRK-B.US`; unmatched rows (2/3) and IBKR-only positions are reported |
| P09 | cash cents equal a BigInt oracle on 15 cases, including half-even ties (0.075 → 0.08; 0.225 → 0.22) and float noise |
| P10 | watcher: off, outside the window, busy lock, HTTP 500 then retry, once per capture, the 12-minute age limit, a clock-ahead capture, an observe refresh sending the off request, and window parsing (wrap, invalid input) |
| P11 | the installer is idempotent; the remover touches only its own triggers |
| P12 | self-test lines; no amounts, ids or packet |
| P13 | a note never carries the receipt phrase and stays at most 400 chars |
| P14 | the newest capture wins; other names and non-JSON files are ignored |

`tests/test_pf_ibkr_evidence.py` feeds what the harness sends to the real backend:
- **Fixed clock.** `certify_portfolio_inputs` certifies the enforce packet: 3 matched rows, holdings and cash certified, and certified cash quantized half-even equal to the request cash. Twelve minutes later the same packet is `invalid_evidence`.
- **Cash cases.** All 15 equal Decimal `(settled - reserved) x 3.75` and the real reconciliation's certified cash.
- **Current clock.** The real `build_portfolio_actions` on the enforce request built by the GAS source returns certified, funding_eligible, execution_ready, 3 trusted rows, and every action `matched`.
- **Each fix is necessary.** Each of these alone withholds:

| Change to the request | Backend reason |
| --- | --- |
| one more cent of cash | `cash_request_mismatch` |
| the lookup FX instead of the peg | `invalid_evidence` |
| no quote provenance | `holding_quote_unverified` |
| the out-of-scope row sent | `custody_unknown` |
| a 16-minute-old capture | `invalid_evidence` |

**Private real-data proof (not in the repository).**
- The v1.5.12 source built the enforce request from the 14 Oct export's My_Portfolio rows and panel, plus a live read-only capture.
- The real build returned `status ok`, certified, funding_eligible, execution_ready, 3/3 trusted rows, and 0 BLOCK.
- A capture written by the scheduled task, in its own fresh cloud session, also validated: linked 3/3, cash certifiable.

## Known limits

- **The certified state lasts 15 minutes.** The backend accepts evidence up to 900 s old. The capture-driven refresh (about 08:45) certifies; the next 4-hourly auto refresh, whose time drifts, has no fresh capture, so the page returns to BLOCK until the next morning.
  - Because the drift can land inside the release window, certification is not guaranteed at release time.
  - The fix belongs in the backend (decision D4: evidence age policy for non-funding actions, or a decision-time snapshot), not in GAS.
  - Raising the age limit in GAS would only be refused by the backend.
- **The panel cash cell is not written.** The request uses the certified cash, but the operator cell `PF: Cash Available SAR` keeps its value. Top_10's `cash=PF` mode reads that cell.
- **The capture is declared, not authenticated.** It is a scheduled session copying the read-only responses verbatim. GAS checks the structure, the Drive creation time and the freshness, and certification cross-checks quantities against the ledger. Cash has no independent cross-check.
- **The 3.75 USD rate is a declared policy proof,** not a market quote. On this page it replaces the lookup rate (3.75547) in enforce mode only.
- **Orders have no currency field.** Reservations assume the account's single cash currency, and anything else withholds the cash.
- **Trigger runtime.** The watcher fires every 5 minutes, but returns at once outside its window.
- **Partial mirror.** Install by replacing this one file in the editor. Never run a full `clasp push` from this partial mirror.

## Operator steps

1. **Merge the PR.** Nothing runs from GitHub.
2. **Install.** Open Extensions → Apps Script → `10_My_Portfolio.gs`, replace the whole content with v1.5.12, and save.
3. **Self-test at once.** Run `pdSelfTest` and approve the Drive permission. It should show:
   - `IBKR evidence mode (PD_IBKR_EVIDENCE): off`
   - `IBKR capture: capture folder not set (PD_IBKR_CAPTURE_FOLDER_ID)`
   - `capture watcher: not installed`
4. **Script Properties.** Set:
   - `PD_IBKR_CAPTURE_FOLDER_ID` to the folder id from the setup message;
   - `PD_EXECUTION_EXCLUDE` = `5023.SR`;
   - `PD_IBKR_EVIDENCE` = `observe`.
   Run `pdSelfTest` again. Expect `IBKR capture: TFB_IBKR_Capture_... -> valid; ...; linked 3/3; cash certifiable` (the age will be hours).
5. **Watcher.** Run `pdInstallCaptureWatcher`.
6. **Next morning.** After the 08:40 capture, by about 08:50 B2 shows ` | [ibkr-observe] would send (capture 08:4x age Nm; linked 3/3; ...)`.
7. **Enforce.** Set `PD_IBKR_EVIDENCE` = `enforce`. After the next capture, B2 shows ` | ibkr evidence SENT (...)`, the ACTIONS carry certified levels, and 5023.SR moves to OUTSIDE EXECUTION SCOPE. Keep `PD_SOURCE_RECEIPT` = `enforce` (v1.5.11) for the receipt.

## Rollback

Delete `PD_IBKR_EVIDENCE` and run `pdRemoveCaptureWatcher`; the page then writes v1.5.11 output. To remove the code, paste v1.5.11 (commit `7c8abdc`). Pausing the scheduled capture task only stops new files.

## Owner decisions

- **D-PF2:** arm IBKR evidence, observe then enforce. Recommended now.
- **D4 (open):** the evidence age policy, so a certified decision survives the 4-hourly refresh. Recommended next.
