# Commit Sheet - approved universe scope v1.0.1 [MF-RETIRE-52] + `16_Decision_Top10.gs` v1.21.1 [MF-SCOPE-2422] - 2026-11-05

**Scope:** the readiness critical path (the brief: "Start with the highest-impact publication blocker"). Owner approval, Thu 5 Nov 2026 12:51 Riyadh: "Approved to be removed if need from me give me the names".
- **The blocker:** sync run 41944918372 was withheld at the final audit: Mutual_Funds name coverage **97.90 %** (2422 of 2474 rows named) against the unchanged **99 %** gate.
- **The cause:** 52 rows that never get a Name. Every one is `acquisition_status: unavailable`. Checked on the Yahoo chart API on 5 Nov ~13:00 Riyadh:
  - 45 return **404** (no longer listed);
  - 5 return **no daily bar in two years** (IBMN, SPVU, IBDQ, HYXU, IBTF);
  - `SPLG.US` has no bar in two years because the fund was renamed **SPYM** (`SPYM.US` trades);
  - the bare `SPLG` row (no venue suffix) is a second copy of the same fund.
  - They are mostly matured iBonds term funds and closed or renamed ETFs. None is held in My_Portfolio.
- **The change (data only):**
  - `config/approved_universe_scope_v1.json`: policy revision `2026-11-05.mf-retire-52.v2`. Mutual_Funds approved count **2474 -> 2422**: the 52 fingerprints are removed and nothing is added. Market_Leaders is unchanged (255, same digests).
  - `16_Decision_Top10.gs` v1.21.1 carries the same revision natively (`DT10_SOURCE_APPROVED_SCOPE`: fund count 2422 and its symbol-set digest). `DT10_SOURCE_MIN_ROWS.Mutual_Funds` follows it (a scoped page's floor is its approved count, as Market_Leaders 255 already is).
  - `finalize_decision_publication.py` 1.1.3: its Mutual_Funds baseline mirrors 2422. The scoped floor already came from the approved scope.
  - The rows leave the page through the sync's existing retirement seam: `_Retired_Symbols` with Page = Mutual_Funds, read by `run_dashboard_sync` v6.64.0 under `TFB_SYNC_RETIRE_TAB = enforce`. The 52 rows are 2.1 % of the page, inside the 20 % cap.
- **Unchanged:** the 99 % name gate, every coverage, freshness and price threshold, every safety gate, verdict, ticket and sizing rule. No broker call.

**Scope digests (Mutual_Funds):**

| | count | membership_sha256 | symbol_set_sha256 |
|---|---|---|---|
| v1 (2026-10-12) | 2474 | `10b7327f802d7c34...` | `d9a3fcb8aefc9de6...` |
| v2 (2026-11-05) | 2422 | `74347d41ddce6b26...` | `1bb8747656e2a4c6...` |

The v2 fingerprint set is exactly v1 minus the 52 below. `tests/test_dt10_v1211_mf_scope.js` K3 proves it: the v2 set plus the 52 reproduces the v1 membership digest.

**Files:**
- `config/approved_universe_scope_v1.json`: revision, `approved_on`, `approval_basis`, the Mutual_Funds count, fingerprints and both digests.
- `scripts/approved_universe_scope.py` 1.0.0 -> 1.0.1: `_PAGE_CONTRACT` Mutual_Funds `("current_fund_tab", 2422, 4496)`.
- `scripts/finalize_decision_publication.py` 1.1.2 -> 1.1.3: `MARKET_FLOORS["Mutual_Funds"] = 2422`.
- `scripts/verify_deployment.py`: the SCRIPTS manifest pins `approved_universe_scope` 1.0.1 and `finalize_decision_publication` 1.1.3 (`tests/test_verify_manifest_pins.py`).
- `apps_script/16_Decision_Top10.gs` 1.21.0 -> 1.21.1: six base lines replaced (the version line, `DT10_VERSION`, the fund floor, the revision line and the fund scope's two lines) plus the WHY block. No function or var is added or removed. Every added line is ASCII; the version line keeps its existing em dash.
- `tests/js/dt10_edits_v1211.json`: the four hunks. Reversing them gives the v1.21.0 file (SHA-256 `e153f4dc`, origin/main 7fb3f77f).
- `tests/test_dt10_v1211_mf_scope.js` (new) and `tests/test_dt10_v1211_mf_scope.py` (new).
- `tests/js/dt10_cash_utc_history.js`: `v1210Source` reverses the v1.21.1 hunks first; every older rebuild starts from it.
- Version pins moved to 1.21.1 in the cockpit harnesses. The refresh-level golden negatives of the v1.19.0, v1.20.0 and v1.21.0 harnesses (and the v1.21.0 lint) read the rebuilt v1.21.0 file: the new fund count reaches the readiness reason in their Logger lines.
- Synthetic fund counts 2474 -> 2422 (unique 2469 -> 2417, pool total 9689 -> 9637) in `decision_source_readiness.test.js`, `dt10_feed_cohort_binding.test.js`, `test_approved_universe_scope.py`, `test_full_refresh_coverage.py`, `test_decision_surface_freshness.py`, `test_final_decision_publication.py` and `test_coherent_real_audits.py`.

## The 52 rows

| # | Symbol (page spelling) | Evidence (5 Nov 2026) | Replacement |
|---|---|---|---|
| 1 | `FM.US` | chart API 404 | - |
| 2 | `USRD.US` | chart API 404 | - |
| 3 | `GXTG.US` | chart API 404 | - |
| 4 | `IBMM.US` | chart API 404 | - |
| 5 | `IBDD.US` | chart API 404 | - |
| 6 | `HEWG.US` | chart API 404 | - |
| 7 | `IBMN.US` | 0 bars in 2 years | - |
| 8 | `MIDF.US` | chart API 404 | - |
| 9 | `SPVU.US` | 0 bars in 2 years | - |
| 10 | `IBDQ.US` | 0 bars in 2 years | - |
| 11 | `VFLQ.US` | chart API 404 | - |
| 12 | `HEWW.US` | chart API 404 | - |
| 13 | `IBCE.US` | chart API 404 | - |
| 14 | `SPLG.US` | 0 bars in 2 years; SPYM.US trades | SPYM.US |
| 15 | `IBTE.US` | chart API 404 | - |
| 16 | `ICOL.US` | chart API 404 | - |
| 17 | `KLD.US` | chart API 404 | - |
| 18 | `IBMK.US` | chart API 404 | - |
| 19 | `IBHA.US` | chart API 404 | - |
| 20 | `IRBO.US` | chart API 404 | - |
| 21 | `DEFA.US` | chart API 404 | - |
| 22 | `HEWC.US` | chart API 404 | - |
| 23 | `IAUF.US` | chart API 404 | - |
| 24 | `KRMA.US` | chart API 404 | - |
| 25 | `IBTD.US` | chart API 404 | - |
| 26 | `HYXU.US` | 0 bars in 2 years | - |
| 27 | `IBTF.US` | 0 bars in 2 years | - |
| 28 | `IBHC.US` | chart API 404 | - |
| 29 | `JKJ.US` | chart API 404 | - |
| 30 | `CZAR.US` | chart API 404 | - |
| 31 | `IBMJ.US` | chart API 404 | - |
| 32 | `IBML.US` | chart API 404 | - |
| 33 | `GXG.US` | chart API 404 | - |
| 34 | `IBHB.US` | chart API 404 | - |
| 35 | `NUSI.US` | chart API 404 | - |
| 36 | `IECS.US` | chart API 404 | - |
| 37 | `IBDM.US` | chart API 404 | - |
| 38 | `SPLG` | same fund as SPLG.US | SPYM.US |
| 39 | `IBTB.US` | chart API 404 | - |
| 40 | `EMBH.US` | chart API 404 | - |
| 41 | `HJPX.US` | chart API 404 | - |
| 42 | `IGN.US` | chart API 404 | - |
| 43 | `IBDN.US` | chart API 404 | - |
| 44 | `JKK.US` | chart API 404 | - |
| 45 | `JKH.US` | chart API 404 | - |
| 46 | `IBDP.US` | chart API 404 | - |
| 47 | `HEWU.US` | chart API 404 | - |
| 48 | `IBHD.US` | chart API 404 | - |
| 49 | `EWGS.US` | chart API 404 | - |
| 50 | `JKE.US` | chart API 404 | - |
| 51 | `FLFR.US` | chart API 404 | - |
| 52 | `HWAY.US` | chart API 404 | - |

## `_Retired_Symbols` rows

Paste below the existing rows (SGH.AX, HKIT.US, RCM.US, SILK.US). Columns: Symbol, Retired On, Reason, Replacement, Page. Tab-separated, 52 rows, no header:

<!-- retired-symbols-paste:begin -->
```
FM.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
USRD.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
GXTG.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBMM.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBDD.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
HEWG.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBMN.US	2026-11-05	no daily bar in 2 years (checked 2026-11-05)		Mutual_Funds
MIDF.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
SPVU.US	2026-11-05	no daily bar in 2 years (checked 2026-11-05)		Mutual_Funds
IBDQ.US	2026-11-05	no daily bar in 2 years (checked 2026-11-05)		Mutual_Funds
VFLQ.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
HEWW.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBCE.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
SPLG.US	2026-11-05	renamed SPYM; no bar in 2 years under SPLG	SPYM.US	Mutual_Funds
IBTE.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
ICOL.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
KLD.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBMK.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBHA.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IRBO.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
DEFA.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
HEWC.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IAUF.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
KRMA.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBTD.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
HYXU.US	2026-11-05	no daily bar in 2 years (checked 2026-11-05)		Mutual_Funds
IBTF.US	2026-11-05	no daily bar in 2 years (checked 2026-11-05)		Mutual_Funds
IBHC.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
JKJ.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
CZAR.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBMJ.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBML.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
GXG.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBHB.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
NUSI.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IECS.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBDM.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
SPLG	2026-11-05	duplicate of SPLG.US (no venue suffix)	SPYM.US	Mutual_Funds
IBTB.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
EMBH.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
HJPX.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IGN.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBDN.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
JKK.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
JKH.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBDP.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
HEWU.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
IBHD.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
EWGS.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
JKE.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
FLFR.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
HWAY.US	2026-11-05	delisted: Yahoo chart API 404 (checked 2026-11-05)		Mutual_Funds
```
<!-- retired-symbols-paste:end -->

The sync reads but does not apply the Replacement column (v6.64.0 scope cut). `SPYM.US` is not added to the page: adding it would change the approved scope again, so it waits for an owner decision.

## Tests
- **`node tests/test_dt10_v1211_mf_scope.js`:** `PASS 7/7`. The battery runs twice with the identical digest `a751752a11d3e971`.
  - **K0:** the four hunks reverse to the v1.21.0 file (SHA-256 `e153f4dc`); both files compile.
  - **K1 inventory:**
    - exactly the six base lines are replaced;
    - the only other added lines are the WHY block (ASCII; WHY / WHAT / GATES / ROLLBACK / Harness);
    - the non-ASCII code points are unchanged;
    - no function or var is added or removed;
    - every prior WHY header is kept.
  - **K2:** the native scope equals the manifest (revision, date, both pages' counts, supersedes floors and symbol-set digests); `DT10_SOURCE_MIN_ROWS` moves only Mutual_Funds, to its approved count.
  - **K3:**
    - the 52 are unique and ticker-shaped, and none is in the v2 scope;
    - v2 plus the 52 reproduces the v1 membership digest `10b7327f`;
    - the paste block above holds exactly the 52, in page order, with Page = Mutual_Funds;
    - `SPYM.US` is the Replacement only for the two SPLG rows.
  - **K4 (the REAL functions):**
    - floors are 255 / 6512 / 453 / 2422;
    - `TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS` = 2474 / 4496 / 1 leaves the fund floor at 2422 and is recorded as superseded;
    - an unscoped floor can still only be tightened;
    - the scope core accepts an approved 2422 set and refuses the un-retired 2474 page, a 2421 page, a duplicate and any set that is not the approved membership;
    - the readiness core VERIFIES the retired 2422 page and names `approved scope 2422` for a 2474 status row.
  - **K5 golden negative:**
    - given the v1.21.0 scope data, the delivered file returns exactly the v1.21.0 outputs (policy under five property sets, 10 scope-core cases, 5 readiness cases, `dt10SelfTest`);
    - with its own data, only the count, revision and digest differ;
    - v1.21.0 withholds the 2422 page (`rows 2422 below floor 2474`); v1.21.1 verifies it.
  - **K6:** 12 of 12 mutants killed: fund count, digest, revision, approval date, fund floor, Global_Markets floor, Market_Leaders count, supersedes floor, header version, `DT10_VERSION`, an unrelated code edit, and the WHY block dropped.
- **Earlier harnesses on the delivered file:** digests are unchanged where they print one.
  - v1.19.0 `PASS 9/9` (`16a2a0106863c61e`), v1.20.0 `PASS 12/12` (`cf2a6a08857bcaaa`), v1.21.0 `PASS 12/12` (`d524bca31bbc45bc`);
  - confirm basis 8/8, pending grace 8/8;
  - funding evidence 103, shadow rank log 86, shadow momentum 365, post-earnings 686 checks;
  - calendar completeness;
  - source readiness 73, board funding 64, feed cohort 21/21, input truth 20 (+3 dual-tree skips), bulk format 9/9, ledger FX 11/11, syntax 7 sources;
  - decision chain 47/47, Plan of Record 62/62, reconciliation inputs 20.
- **`pytest`** (the scope, coverage, freshness, finalizer, release-feed, render-settle/hold and cockpit-wrapper files, 35 in all): 727 passed, 13 skipped, 56 subtests passed.

## Operator steps
1. **Merge** the PR.
2. **Paste** the full `apps_script/16_Decision_Top10.gs` v1.21.1 into Apps Script and Save. `dt10SelfTest` starts with `[DT10 v1.21.1] self-test`.
3. **`_Retired_Symbols`:** done by Claude on Thu 5 Nov 2026 13:43 Riyadh, under the owner's approval: the 52 rows above, in rows 6-57, Page = Mutual_Funds.
   - A read-back (gviz) shows the header, the 4 Global_Markets rows and the 52 in page order.
   - The sync's own parser (`_retired_index_from_matrix` / `_retired_for_page` / `_retire_decide`) on that matrix gives Mutual_Funds 52 (both SPLG rows), Global_Markets 4, junk 0, verdict `retired`.
   - Do not delete Mutual_Funds rows by hand: the sync removes them on its next write, and the tab keeps them out.
4. **The next sync** (scheduled 15:17 or 23:17 Riyadh, or a fresh `daily_sync` dispatch after 15:00 Riyadh) removes the 52 rows: the log shows `[UNIVERSE-RETIRE v6.64.0] ... Mutual_Funds ... verdict=retired` and `_Status` adds ` retired=52`.
5. **Check:**
   - Mutual_Funds has 2422 rows and 100 % names;
   - the final audit passes the fund page;
   - Top_10_Investments carries `source readiness VERIFIED (run N)`.
   - Steps 1-3 must all land before the same sync. Until then every path fails closed: a page/policy mismatch withholds the board, which is today's state as well.
6. **Rollback:**
   - revert the PR;
   - Project History -> 16 v1.21.0;
   - delete the 52 `_Retired_Symbols` rows and paste the 52 symbols back into Mutual_Funds (the sheet is the symbol source; the sync does not re-add them).

## Known limits
- The workflow env defaults `EXPECTED_MIN_ROWS_MUTUAL_FUNDS || '2474'` (5 workflows) stay. The approved scope supersedes them for this page and the audits record them as superseded. The advisory `tfb_export_audit` / `tfb_workbook_dq_audit` EXPECT tables also stay at 2474; they already carry Global_Markets 6609.
- A fund that comes back (relisted) needs its `_Retired_Symbols` row removed, the symbol pasted back into the page, and a new scope revision.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
