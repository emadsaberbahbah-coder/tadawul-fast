# TFB Commit Sheet - run_dashboard_sync v6.70.0 [FW-2 SCOPE]

Date: 2026-10-14. Base: origin/main `15812b6` (#765). Lane: GitHub Actions (the sync runs in daily_sync.yml). Gate: repository Variable `TFB_SYNC_FW2_SCOPE` = off | observe | enforce, default off = v6.69.0 byte-identical. Nothing is armed by this change.

## Why

The 2026-10-14 workbook export (sync run 38486122061, stamps 02:46-03:43 Riyadh) shows the FW-2 outgoing row firewall stripping **114** Global_Markets rows in one leg. The two legs before it stripped 60 (13 Oct 15:54) and 102 (13 Oct 23:23). This is Q3 earnings season. FW-KEEP restored **38** rows from last-good. The other **76** were written as symbol-only stubs: every cell blank except Symbol, Warnings = `identity_quarantined:v6.24.0`. Their previous sheet row was already a stub, so no last-good existed.

The 76 stubs are not delisted names. They include JNJ, AMAT, CDW, FLEX, PBF, INFY and WIT, plus Samsung Electronics (005930.KS), Tokyo Electron (8035.T), Givaudan (GIVN.SW) and Remy Cointreau (RCO.PA). The 38 restored rows include Citigroup, ICBC, CCB, ABC, BoCom and SK Hynix.

FW-2 strips a row when |Price/EPS - P/E| / P/E >= 5% and the ratio is outside the 50-200x GBX unit band. P/E and EPS reach the sheet from different providers and vintages, so one side of a fresh earnings print, or an H-share's HKD price over its CNY EPS, trips it on a healthy row. The v6.25.1 WHY block in this file already records that ("mixed-vintage fields ... trip it on perfectly real blue-chips").

The repository's real final-publication audit (`finalize_decision_publication.run`, publish=False), replayed offline on the same export, shows what the stubs cost:

| Effect on the 14 Oct cohort | Measured |
| --- | --- |
| GM name coverage | 97.00% (198 nameless rows), below the 99% gate. 75 of the 198 are FW-2 stubs; the 76th stub (RYOJ.US) kept its name. |
| GM timestamps | 76 blank, unparseable or date-only, all of them stubs. That is a coverage WARNING, so exit 1, and the P0-1 release refuses anything but [0, 0]. |
| Counterfactual: stubs keep their rows, other blockers fixed | coverage exit 0 is reachable only once the stubs are gone (scenario S8 below). |

## What changed

`scripts/run_dashboard_sync.py` 6.69.0 -> 6.70.0. 14 anchored edits, each asserted `count == 1` on the pinned base sha256 `021e1ced5dee...`.

- `_row_identity_firewall(headers, rows_matrix, _mode=None)`: the trip test is unchanged. Under observe or enforce each tripped row is classed by implied/stated P/E ratio: **drift** when 0.5 <= ratio <= 2.0, otherwise **gross**.
  - observe: strips exactly as v6.69.0 and records a census (mode, drift, gross, fielded, and up to 60 rows with symbol, currency, price, EPS, P/E, ratio, tier and action).
  - enforce: a drift row is kept. P/E (TTM), EPS (TTM) and PEG are cleared, Investability Status = BLOCKED, Final Action = DO_NOT_INVEST, a Block Reason note is prepended and `fw2_pe_eps_drift:v6.70.0` is appended to Warnings. Every other cell (Symbol, Name, quote, timestamps, scores) is untouched. Gross rows are stripped and FW-KEEP restores last-good exactly as before.
- `_append_runlog_idfirewall(..., fw2_census=None)`: with a census, the `[ID-FIREWALL]` _Run_Log line gains ` | fw2=<mode> drift=D gross=G fielded=F` and the Details JSON gains a `fw2` block. The line is WARNING/SUSPECT when anything was fielded or grossly stripped. With None, the output is identical.
- Call site (`_run_one_task`): reads the page's census immediately after the firewall, with no await in between. One `[FW-2 SCOPE v6.70.0]` warning is added when rows were fielded. The census is passed to FW-3.
- New pure helpers: `_fw2_scope_mode_raw`, `_fw2_scope_mode`, `_fw2_tier`, `_fw2_field_scope_row`, `_fw2_selftest`. enforce degrades to observe when the cached self-test fails.
- `.github/workflows/daily_sync.yml` VERSION 1.0.3 -> 1.0.4: `TFB_SYNC_FW2_SCOPE: ${{ vars.TFB_SYNC_FW2_SCOPE || 'off' }}` in both writer jobs (sync-dashboard, recover-missing-market-pages), so recovery cannot undo the primary leg.
- `tests/test_workflow_flag_mappings.py`: `TFB_SYNC_FW2_SCOPE: off` added to SYNC_DEFAULTS. The existing parity tests now cover it.
- `scripts/verify_deployment.py` 1.0.60 -> 1.0.61: pin run_dashboard_sync 6.69.0 -> 6.70.0, pin only.
- `.github/workflows/ci.yml` 1.1.19 -> 1.1.20: the new harness joins the blocking lean list.
- New `tests/test_sync_fw2_scope.py` (10 cases) and `tests/fixtures/fw2_scope_gm_rows_2026-10-14.json`: 50 real Global_Markets rows copied verbatim from the export, market data only.

Why the tag is safe. `fw2_pe_eps_drift:v6.70.0` contains none of the substrings the engine investability gate tests (cap, forecast, target, roi, drop, reject, provider_target, price_bar_stale, xprovider_price_conflict). It is also not an invalid-acquisition token in `core.data_validity._INVALID_WARNINGS`, so a freshly acquired price still counts as acquired. The row is fenced by BLOCKED / DO_NOT_INVEST, not by the tag.

## Default OFF identity

With the Variable unset, the workflow maps `off`. `_row_identity_firewall` then runs its v6.69.0 logic: it reads one extra env value and writes no census. FW-3 receives `fw2_census=None` and builds the same message and the same Details JSON in the same key order. The call site does nothing else. Harness T02 proves value-for-value equality of the firewall output with the v6.69.0 module on the case matrix. T10 proves the FW-3 append body equals v6.69.0's, apart from the version string. AST inventory: 370 -> 375 definitions, **0 removed**. Smart quotes 0 -> 0, non-ASCII characters 566 -> 566.

## Measured results

Python 3.11.17 with the pinned requirements.txt.

```
TFB_TEST_SYNC_BASE=<v6.69.0 copy> python tests/test_sync_fw2_scope.py   (x3)
PASS 10/10 digest 921c6538269231a7   (identical x3)
python -m pytest -q tests/test_sync_fw2_scope.py                          8 passed, 2 skipped (base cases)
TFB_TEST_SYNC_BASE=<v6.69.0 copy> python -m pytest -q tests/test_sync_fw2_scope.py   10 passed
golden negative: TFB_TEST_SYNC_DELIVERED=<v6.69.0 copy> pytest              6 failed, 2 passed, 2 skipped
python tests/test_verify_manifest_pins.py                                 [MANIFEST PIN GUARD] 3/3 PASS
```

The case matrix is the 50 real rows, which all pass FW-2 at HEAD (T01), plus deterministic perturbations of copies:

- EPS vintage drift, +12% on NVDA, AAPL, JPM and C (drift)
- stale P/E, -15% on KO and PG (drift)
- HKD price over CNY EPS, EPS / 1.09, on 0939.HK, 1398.HK and 3988.HK (drift)
- three transposed enrichment blocks (MSFT<-KO, WMT<-MSFT, XOM<-DUOL), classed by their own ratio.

Under enforce, drift rows change only the seven fenced cells (T04). Gross rows equal the v6.69.0 strip byte for byte (T05). The existing 9-case `_idfw_selftest_` still passes in every mode (T08).

Offline final-publication replays on the 14 Oct export (real audit code, edited copies of the export; scripts kept with the 14 Oct review):

| Scenario | Audit codes | Remaining blockers |
| --- | --- | --- |
| S0 as exported | [2, 2] | GM names 97.00%; PF and T10 receipts; T10 pool count and reconciliation; false full-universe claim |
| S3 cockpit + PF receipt + names back on the 75 nameless stubs | [2, 0] | GM names 98.14% (the 123 no-data rows) |
| S6 ... + all 198 GM names repaired | [1, 0] | 76 blank stamps (warning). A release refuses this. |
| S8 ... + the 76 stub rows written as real rows (name + stamp) | **[0, 0]** | none |

FW-2 SCOPE under enforce produces the S8 shape for every stub whose ratio falls in the drift band. The observe census measures how many that is before anything changes.

Whole suite on the branch: `python -m pytest -q tests` -> **4911 passed, 38 skipped, 0 failed, 243 subtests passed** in 565.6 s, exit 0.
`scripts/audit_repository_workflows.py`: errors=0 warnings=2. Those are the same two daily_sync TEMPORARY_OVERRIDE_ACTIVE warnings as before; their line numbers moved because of the new env lines.
`scripts/harness_w1a6.py --deterministic`: 94/94 PASS.

## Known limits

- A transposed enrichment block whose two issuers' prices lie within 2x of each other lands in the drift band. In enforce it stays visible with the other issuer's Name and fundamentals residue, but BLOCKED / DO_NOT_INVEST, so it can never reach a ticket. FW-4 name-dedup and the v6.30.0 exact critical identity registry still run after FW-2. The observe census (ratio per row) measures this risk before enforce.
- Drift rows are BLOCKED, so they cannot become board seats under enforce. Today they are invisible stubs, so nothing is lost. Whether drift rows should stay investable is a separate owner decision.
- The 0.5-2.0 band is a constant, not an env knob, so the band cannot drift silently. Changing it is a code change.
- The census holds up to 60 rows per page; counts are always complete.
- Other quarantine paths (FW-5 fabricated placeholder, name-dedup, critical identity) are not touched.

## Operator steps

1. Merge the PR. No Render deploy is needed: the sync runs in GitHub Actions.
2. Set the repository Variable `TFB_SYNC_FW2_SCOPE=observe`: [Settings, Variables](https://github.com/emadsaberbahbah-coder/tadawul-fast/settings/variables/actions).
3. Read-back on the next scheduled sync: _Run_Log `[ID-FIREWALL v6.70.0] Global_Markets ... | fw2=observe drift=D gross=G fielded=0`, and the Details JSON `fw2.rows` (ratio per stripped row). Expect drift to be most of `out_stripped`.
4. Owner decision on enforce after one observe read-back: `TFB_SYNC_FW2_SCOPE=enforce`. Read-back on the next sync:
   - `fielded=F > 0`;
   - the stubbed GM symbols reappear with Name and Last Updated, BLOCKED, with `fw2_pe_eps_drift:v6.70.0` in Warnings;
   - GM nameless rows fall by F;
   - the coverage audit's blank-timestamp warning falls by the same F.

## Rollback

Unset the Variable, or set `off`. The next sync writes v6.69.0 behavior. To remove the change, revert the commit; nothing else depends on it.

## Owner decisions

- D-FW2a: arm observe (recommended now; zero row changes).
- D-FW2b: arm enforce after one observe read-back shows the drift/gross split.
