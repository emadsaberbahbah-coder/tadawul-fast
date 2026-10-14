# TFB Commit Sheet - 10_My_Portfolio.gs v1.5.13 [RECEIPT BOUND TO THE ROWS READ]

Date: 2026-10-14.
- **Base:** v1.5.12 (`0530413`, sha256 `7161b974...`). Result sha256 `a8a00d6d...`, 4,529 lines.
- **Lane:** Apps Script (bound project). A paste of one file. It supersedes v1.5.12 for installation.
- **Gate:** it applies only when `PD_SOURCE_RECEIPT` is observe or enforce. Off still never reads `_Status`, and request, render and status line stay v1.5.11 byte-identical.

## Why

Codex raised a P1 on PR #776, and the 14 Oct export confirms it. The v1.5.11 receipt bound only the `_Status` row, never the holdings it consumed.

The export shows this sequence:
- The sync published My_Portfolio at **02:46** Riyadh (run 38486122061, acquired 4/4, COMPLETE).
- The GAS auto refresh (`tfbAutoRefreshTrigger` then `refreshPageInBatches`, `_Run_Log` 07:40:05 to 07:41:47) rewrote every My_Portfolio row at **07:40**. Last Updated (UTC) reads 04:40:17Z. `_Status` was not touched.

The 08:50 decision therefore read 07:40 rows, yet v1.5.11 would have written `source readiness VERIFIED (run 38486122061)`. That is an overclaim the final audit cannot see, because it reads only `_Status` and B2. A sync that rewrites rows before stamping `_Status` is the same blind spot.

## What changed

There are 11 anchored edits on the pinned base, each asserted `count == 1` (`scripts/build_pf_binding.py` in the review kit).

**Per-row timestamps.**
- `pdCollectHoldings_(extraFields, wantStamps)` returns `stamps: [[SYMBOL, ms], ...]` from each consumed row's Last Updated (UTC), but only when asked and only when the column exists.
- New `pdSrcUtcMs_` reads that column: a Date keeps its instant, text needs a full time, and a naive time is UTC (not Riyadh).

**The binding.** New pure `pdSrcBindingCore_(pre, post, stampMs)` requires:
- every consumed row to lie inside the run window `[publication - 3 h, publication + 2 min]`;
- the same rows, re-read after the decision, to be unchanged.

**Withheld reasons.** When the binding fails, the receipt is withheld with one of these:

| Reason | When |
| --- | --- |
| `holdings refreshed after the publication (HH:MM > HH:MM Riyadh)` | a row is newer than the run window |
| `holdings older than the publication` | a row is older than the run window |
| `holdings changed during the decision` | the re-read differs from the rows consumed |
| `holdings re-read failed` | the re-read could not be made |
| `holding timestamp unverified (SYM)` | a row's timestamp cannot be read |
| `holdings timestamps unavailable` | there is no Last Updated (UTC) column |

**Other changes.**
- `pdSrcReadinessCore_` exposes the publication instant as `stamp_ms`.
- `pdReceiptNote_` takes the binding as an optional sixth argument, checked only when the publication is otherwise ready.
- `refreshPortfolioDecision` requests the timestamps when the receipt is armed and re-reads them after the render.
- `pdSelfTest` adds `receipt binding (page rows vs run N): inside the run window | WITHHELD: why` for the rows on the page now.

**Totals.** Top-level functions go from 116 to 118: 2 added, 0 removed, 5 changed. Two vars are added (`PD_SRC_BIND_EARLY_MS`, `PD_SRC_BIND_LATE_MS`). The source is ES5, added lines are ASCII, and the non-ASCII count stays at 102.

## Measured results

```
node tests/js/pf_source_receipt.test.js   (x3)  PASS 8/8 digest 9cc07d4571cda7ad   (T04 now 26 withheld cases)
node tests/js/pf_ibkr_evidence.test.js    (x3)  PASS 14/14 digest e67efae298e3d8bb  (P04 adds the refreshed-rows case)
golden negative: the same receipt harness on v1.5.12 and on v1.5.11 fails T04 at
  "rows refreshed after the publication (GAS auto refresh, 14 Oct shape)" (VERIFIED on the base)
binding mutations (ignored, no re-read compare, no late or early bound, naive read as Riyadh, stamps when off): 6/6 caught
python -m pytest -q tests/test_pf_ibkr_evidence.py tests/test_pf_source_receipt.py   11 passed
python -m pytest -q tests  (whole suite, branch = v1.5.13 + main 030e22e)        5112 passed, 33 skipped, 0 failed
real 14 Oct rows vs the 02:46 publication -> WITHHELD: holdings refreshed after the publication (07:40 > 02:46 Riyadh)
```

The new receipt cases are:
- rows refreshed at 07:40 after a 02:46 publication (the export's shape);
- rows older than the run window;
- rows rewritten during the decision;
- a row with no timestamp;
- no Last Updated (UTC) column;
- a naive UTC value read as UTC;
- the self-test binding line.

## Effect and owner decision (D-PF3)

On today's schedule the receipt is correctly **WITHHELD** at the morning decision. The 4-hourly GAS My_Portfolio refresh (about 03:35 and 07:40) replaces the sync's 02:46 publication before the decision reads the page.

The decision itself is unaffected: IBKR certification does not depend on the receipt. Only the final-publication release, which needs the receipt, stays blocked. It is also blocked until about 12 Nov by precondition A, unless D11 waives that.

To make a receipt possible, one of these is needed:
- **(a)** pause the My_Portfolio GAS auto refresh, so the page holds the sync publication through the morning decision (within the 8 h window); or
- **(b)** have that refresh stamp `_Status` with its own run and acquisition receipt. That change is in the auto-refresh file, which is not in this mirror.

## Known limit

An edit that leaves Last Updated (UTC) untouched cannot be detected without a producer-issued digest of the written rows that GAS can reproduce. A natural follow-up is a symbol-plus-timestamp digest in the sync's status stamp.

## Install and rollback

Paste v1.5.13 instead of v1.5.12; the steps are the same as in the v1.5.12 sheet. To roll back, set `PD_SOURCE_RECEIPT` to off, or paste v1.5.12.
