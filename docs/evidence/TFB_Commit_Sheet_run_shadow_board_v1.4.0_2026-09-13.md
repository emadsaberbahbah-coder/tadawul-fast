# TFB Commit Sheet — scripts/run_shadow_board.py v1.4.0 + shadow_board.yml — 2026-09-13

## Register item
**P-139** — S-1 challenger starvation: board sources candidate ROI from a display
surface (Top_10 first table) whose ROI cells are deliberately blank on
carried/grace rows → every candidate NO_ROI → Gen2 Eligible NO → `chal fresh=0/0`.
Evidence: operator Shadow_Board paste 2026-09-13 20:24 (9/9 rows blank-ROI/NO_ROI/NO);
root pinned in code (`rows_to_records` → `roi_pct=None` → verdict NO_ROI, L388–399).
Falsifies and retires the switch-scan-coupling hypothesis. Adjudicated with the
operator: **Option A — board sources Engine ROI itself.**

## Fix (v1.3.1 → v1.4.0)
- `TFB_BOARD_ENGINE_ROI` = **off** (default) | **observe** | **enforce**
- `fetch_engine_roi_map` — reads Expected ROI 12M from Global_Markets → Market_Leaders
  (header-resolved, first hit wins, per-tab errors reported never raised; 2 col reads/tab)
- `_parse_engine_roi_cell` — display-string unit guard: `%`-strings = points; bare
  `0<|v|<1` = the known fraction-scale residual class → ×100, tagged `fraction_fixed`;
  `|v|>300` dropped (`capped`, ROI-cap doctrine); arrows/commas stripped
- `apply_engine_roi` (pure) — observe: annotate new **ROI Src** column only
  (`obs:<v>`, `†`=fraction), **eligibility math byte-unchanged**; enforce: fill blank
  `roi_pct`, tag `engine`/`engine/frac`; off: true no-op
- Meta line: `engine_roi: mode/applied/fraction_fixed/unresolved [+errors]`
- **Schema constraint honored:** `ROI Src` inserted **before** `Gen2 Eligible` because
  both `eligible_symbols()` here **and scorer v1.8.0 L1996 read Gen2 positionally as
  `r[-1]`** — Gen2 stays the last column. Column present in all modes (stable schema);
  off-mode = one empty cell vs v1.3.1 (stated deviation from byte-identical-off).

## Pins
| Artifact | Base | Delivered |
|---|---|---|
| run_shadow_board.py | `4502e979…` (771 L) | **v1.4.0 `3eb901a3…`** (944 L) |
| shadow_board.yml | `adf932c6…` | **armed-observe `2a2a971c…`** *(re-tagged P-139: final sha in repo)* |

## Audits
- 10 anchored edits + 1 contract fix, each count==1 · py_compile PASS · AST 25→29
  fns (4 added, **0 removed**) · smart-quotes 0 · workflow YAML parse PASS
- Selftest extended in-file (runs in CI with real deps): parse battery, apply modes,
  Gen2-last-column geometry, positional asserts updated [16]→[17]
- **Dual-tree stubbed harness ×3, digest `3b2c203c…`**, driven by tonight's real
  9-row evidence shape: off = byte-equal to base modulo the empty ROI Src cell;
  observe = 7 annotations (1 fraction-fixed†, 2 honest unresolved), eligibility
  unchanged; **enforce = 7 NO_ROI rows melt to TRADE/Gen2 YES** (ADAM via
  fraction-fix), 2 stay NO_ROI; `eligible_symbols` r[-1] contract proven both trees;
  env plumbing (bogus→off)

## Deploy (operator — one commit, GitHub lane)
1. Commit to `main`: `scripts/run_shadow_board.py` (v1.4.0) ·
   `.github/workflows/shadow_board.yml` (observe armed) ·
   `docs/evidence/TFB_Commit_Sheet_run_shadow_board_v1.4.0_2026-09-13.md` ·
   `tests/test_board_engine_roi_p139.py`
2. Nothing else — no Render, no properties.

## Read-back & arming path
- **Observe (Step 1, this commit):** Monday 05:10Z (~08:10 Riyadh) board — meta shows
  `engine_roi: mode=observe applied=a/b…`; grace rows carry `obs:<v>` in ROI Src;
  Gen2 still NO everywhere (by design)
- Watch item: Monday 18:20 scorer log — confirm shape-guard/forks digest the 18-col
  board + extra meta row cleanly (r[-1] contract preserved by construction)
- **Enforce (Step 2, separate sitting after clean observe):** flip env → challengers
  exist → **record the S-1 window boundary annotation on the flip date** (EODHD
  precedent; at 3/28 scored days the reset cost is negligible)
- Rollback: env `off` or git revert
