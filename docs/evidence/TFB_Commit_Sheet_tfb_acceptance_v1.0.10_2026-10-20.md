# TFB Commit Sheet - tfb_acceptance v1.0.10 [LIVE READ WINDOW FOR THE DECISION-INTEGRITY CHECKS]

Date: 2026-10-20 (daily improvement, one item, priority (b): a silent pipeline that hides a decision-surface defect class).

- **Base:** main `aa1f440c`. `scripts/tfb_acceptance.py` 1.0.9 (sha256 `dc9da210f58dd742...`, 1,322 lines). Result: 1.0.10 (sha256 `bd187c7a78a398d0...`, 1,397 lines).
- **Lane:** GitHub Actions (`acceptance_check.yml`, read-only measurement). Nothing in Render, the backend or Apps Script changes.
- **Gate:** repository Variable `TFB_ACCEPTANCE_LIVE_WINDOW`, mapped at job level with default `'0'`. `1/true/yes/on` turns it on; any other value is off.
- **Off:** the live reader is v1.0.9 byte-identical: the same worksheet calls in the same order and the same check rows. This is proven against the base copy.

## Why

The acceptance instrument is the daily measured "definition of done". On the live workbook, four of its decision-integrity checks have never measured anything. TFB Acceptance run 39420446126 (2026-10-20 12:12Z, job 118619770079) prints:

```
A1                 board seats on Derayah-tradable venues ...   NA   Top_10 board section not found
A2a                USD FX peg - Top_10 audit Price SAR/Price ... NA   Top_10 audit section not found
A3-Global_Markets  ... equity identity collisions ...            NA   Symbol/Name columns not found
A3-Market_Leaders  ... equity identity collisions ...            NA   Symbol/Name columns not found
TALLY PASS=28 WARN=6 FAIL=1 NA=7 NA_hard=6 NA_design=1
```

These are 4 of the 6 hard NAs. Since v1.0.7, any hard NA makes the overall verdict FAIL, so the verdict is FAIL every day whatever happens. A real untradable seat on the board, a USD row off the SAR peg in the audit, or a futures or crypto name on an equity row could not change the verdict.

**Cause.** The checks are correct; the live reader gives them too little data.

| Check | Parses | Live reader v1.0.9 gives it | 14 Oct export geometry |
| --- | --- | --- | --- |
| A1 | the board header (a row with Symbol and Stability) | `Top_10_Investments` rows 1-60 x columns 1-12 | the board header is row 19 and 30 columns wide, with Stability in column 25 |
| A2a | the header after the `CANDIDATES` row | the same 1-60 x 12 window | `CANDIDATES` is row 60 or 61, and the audit runs to about row 560 (top 500 written) |
| A3 | Symbol + Name of each equity page | 11 batch-read page columns, without Name | Name is column 2 |

The export path reads whole tabs, so `--selftest` and `--export-dir` have always passed. Only the live run is blind.

**What the four checks say once they can read.** This file's export path over the 14 Oct export (`--export-dir`):

| Check | Verdict | Measured |
| --- | --- | --- |
| A1 | PASS | 0 (4 seats, all on Derayah venues) |
| A2a | **WARN** | median **3.7424**; 7 of 388 USD audit rows outside [3.74, 3.77] (BTG.US 3.7379, EVH.US 3.7380, ICL.US 3.7380, ...) |
| A3-Global_Markets | PASS | 0 (6,028 equity rows) |
| A3-Market_Leaders | PASS | 0 (255 equity rows) |

The harness's G1 case reads that export through the delivered live reader (gate on) and gets exactly these four rows.

## What changed

`scripts/tfb_acceptance.py` 1.0.9 -> 1.0.10. Header WHY block added; all prior blocks are kept verbatim.

- **`_live_window_enabled()`** (new): reads `TFB_ACCEPTANCE_LIVE_WINDOW`; `1/true/yes/on` means on.
- **`Source.integrity_tab(name)`** (new): returns `tab(name)`. The export, xlsx and in-memory sources therefore behave exactly as before.
- **`LiveSource.integrity_tab(name)`** (new):
  - off, or a tab other than `Top_10_Investments` and the equity pages: returns `tab(name)`, which is the v1.0.9 read;
  - on, `Top_10_Investments`: reads rows 1..row_count x 40 columns (clamped to the grid);
  - on, an equity page: reads the `Symbol` + `Name` columns (one header read plus one `batch_get` of 2 ranges).
  - Each window result is cached under its own key. A failed read is recorded in `.errors` (fail-closed, like `tab()`), and the check reports NA.
- **`LiveSource._page_columns(ws, cols=None)`**: the new optional argument names the columns to read. Without it, the function is v1.0.9.
- **`_top10_sections`** (used only by A1/A2a) and the **A3** loop now read `integrity_tab`.
- **Unchanged:** every other check, including D10-1, D10-1b/1c and all G checks; their reads stay `tab()`. No threshold, band or check logic changes.
- **Totals:** functions 56 -> 59 (3 added, 3 changed, **0 removed**). Added lines are ASCII.

`.github/workflows/acceptance_check.yml` 1.0.1:
- a dated WHY block;
- a job-level `env:` that maps `TFB_ACCEPTANCE_LIVE_WINDOW: ${{ vars.TFB_ACCEPTANCE_LIVE_WINDOW || '0' }}`.

It is deliberately placed outside the "Measure the live workbook" step, which draft #795 edits, so the two do not conflict.

## Measured results

```
pytest tests/test_acceptance_live_window.py (TFB_TEST_ACC_BASE + TFB_TEST_ACC_EXPORT)   12 passed
python3 tests/test_acceptance_live_window.py   (x3, same env)        PASS 12/12 digest 37f221aef4a62627
python3 tests/test_acceptance_live_window.py   (CI: no base, no export) PASS 9/12 (skipped 3), same digest
golden negative: the harness with the v1.0.9 copy as the delivered module   8 failed, 1 passed (N4), 3 skipped
G1 real 14 Oct export, live reader on == export path: A1=PASS:0 | A2a=WARN:3.7424 | A3-GM=PASS:0 | A3-ML=PASS:0
mutations 8/8 caught: gate ignored, sections keep tab(), A3 keeps tab(), 12 columns, 60 rows,
  fault not recorded, cols ignored, shared cache key
  (a 9th, adding Portfolio_Decision to the window set, is equivalent: nothing asks integrity_tab for it)
python3 scripts/tfb_acceptance.py --selftest   PASS 8/8 (unchanged)
compileall main.py core routes scripts tests   ok
ci.yml lean list                               see the PR
```

**Harness cases** (`tests/test_acceptance_live_window.py`). It drives the REAL `LiveSource`, built without credentials, over a fake gspread layer that reproduces the 14 Oct geometry and gspread's trimming of trailing empty cells and rows.

- **W1:** on, A1/A2a/A3 measure on the live geometry.
- **W2:** each defect class is caught:
  - an untradable `.NS` seat;
  - a USD median of 3.809;
  - `KE.US` "Wheat Futures" and `LINK.US` "Chainlink USD", while `CC.US`, `FTFT.US` and `NG=F` are not flagged.
- **W3:** every other check row is unchanged on vs off (at least 20 rows); only the A rows move from NA(parse) to measured.
- **W4:** the extra reads are exactly 1 Top_10 get (`A1:AD<n>`), plus per page 1 header get and 1 `batch_get` of `A2:A<n>`, `B2:B<n>`. The v1.0.9 calls stay in order.
- **W5:** a failed window read is recorded in `.errors` and the overall verdict is FAIL; check_board keeps its own read.
- **W6:** the export and in-memory sources ignore the gate. The live window gives the same A rows as the export path and the in-memory source on the same grid.
- **N1:** off (unset and 7 non-truthy spellings) equals the v1.0.9 base copy in check rows, errors and the full call log.
- **N2:** golden negative. The base copy leaves A1/A2a/A3 NA(parse) on the same live grid, and W1 fails on it.
- **N3:** truthy spellings, the version floor (>= 1.0.10), and the job-level workflow mapping with default `'0'` outside the live step.
- **N4:** the inline selftest still passes 8/8.
- **N5:** added lines are ASCII.
- **G1:** the real export grid: base live gives NA; delivered live (on) equals the export path.

## Operator steps (owner) - nothing is armed by this PR

1. Merge. No deploy is needed; this runs in GitHub Actions.
2. **Proposed arming (owner decision):** Settings > Secrets and variables > Actions > Variables, set `TFB_ACCEPTANCE_LIVE_WINDOW = 1`.
3. Read back the next `TFB Acceptance` run (09:45 Riyadh, or dispatch it):
   - A1, A2a, A3-Global_Markets and A3-Market_Leaders show PASS/WARN/FAIL with measured values instead of NA;
   - `NA_hard` drops by 4 (from 6 to 2 on today's state).
4. **Rollback:** unset the Variable.

## Known limits

- The overall verdict stays FAIL today:
  - D10-3b (2,945 overdue cohorts) is a real FAIL;
  - D10-1b/1c are NA because today's Top_10 status line carries no SEAT-CHECK and the output is WITHHELD.
  - Those come from a different parser (`_top10` / `_top10_execution`) and are not a read-window problem, so they are not touched here.
- **A2a on the 14 Oct export is WARN**, and the median implied USD->SAR rate in the Top_10 audit is 3.7424 (280 of 388 rows at 3.742). That is 0.2% under the 3.75 peg, inside the [3.74, 3.77] band. This PR only makes the check visible; the FX source behind `Price SAR` is reported for an owner look, not changed.
- **Read size when on:** Top_10 rows 1..row_count x 30 columns (40 requested, clamped) in one `values.get`, plus 2 columns x about 6,600 rows for Global_Markets and 2 x 255 for Market_Leaders. These are read-only service-account reads, three or four calls in all.
- **CI coverage:** the harness is not enrolled in the blocking lean lane. `ci.yml` is edited by open #778 and #801, so enrolling here would collide with their version lines. The advisory whole-suite job collects it, and `acceptance_check.yml` still runs the inline `--selftest`. Enrolling it is proposed once #778 lands.
- **Golden-negative cases N1/N2 need the base copy** (`TFB_TEST_ACC_BASE`), and G1 needs a local export (`TFB_TEST_ACC_EXPORT`). They skip in CI and were run locally, as recorded above.
