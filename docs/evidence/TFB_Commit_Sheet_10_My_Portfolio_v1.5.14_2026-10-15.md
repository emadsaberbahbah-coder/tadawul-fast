# TFB Commit Sheet - 10_My_Portfolio.gs v1.5.14 [PANEL INPUTS SURVIVE A REBUILD]

Date: 2026-10-15 (daily improvement, one item).
- **Base:** v1.5.13 (main `11ef1345`, sha256 `a8a00d6d...`, 4,529 lines). Result sha256 `92630e99...`, 4,639 lines.
- **Lane:** Apps Script (bound project). A paste of one file. Nothing in the backend, workflows or Render changes.
- **Gate:** Script Property `PD_PANEL_PRESERVE` = `off` (default) | `observe` | `enforce`. Any other value reads `off`. When off, the builder reads no extra cell and every sheet write is v1.5.13 byte-identical.

## Why

The Portfolio_Decision control panel lost the owner's inputs.

| Source | PF: Cash Available SAR | PF: Max Position % |
| --- | --- | --- |
| `_Portfolio_Action_Log` panel snapshot, 2026-10-14 04:45:58 Riyadh | 53,769.2625 | 20 |
| Workbook export, 2026-10-15 | 0 | 15 |
| `_Lists_Config` / built-in defaults | 0 | 15 |

The same reset happened once before. On 2026-09-09 at 23:14:30 the panel read cash 0 and Max Position 15, and the owner retyped 19,615 and 20 at 23:25:05.

**Cause.** `buildPortfolioDecisionLayout()` calls `sheet.clear()` and then re-seeds every panel cell from the defaults. `refreshPortfolioDecision()` calls it on its own whenever A4 is blank. One rebuild therefore:
- sizes every ADD against 0 SAR of cash;
- applies a 15 % position cap the owner never chose.

16_Decision_Top10 fixed the same defect in v1.11.3 (P-76, `dt10PanelPreserve_`), but the port never reached this page. The Top_10 cockpit also reads this page's cash cell (P-134 cash source) and its `PF: Max Position %` cell (WP-8 input truth). When either mode is on, the reset reaches Top_10 too.

## What changed

3 anchored edits on the pinned base, each asserted `count == 1`:
1. Header version, plus a new v1.5.14 WHY block above v1.5.13 (all prior WHY blocks verbatim).
2. `PD_VERSION` is now `'1.5.14'`.
3. The builder and three new functions, described below.

**New functions.**
- `pdPanelPreserveMode_()` reads the Script Property the same way as `PD_SOURCE_RECEIPT` (trim, lower case; a fault reads off).
- `pdPanelPreserve_(sheet)` snapshots the panel's NON-BLANK values before the clear. A value is kept only when its label cell still holds the exact `PF:` label at the same position, so a moved or renamed layout keeps nothing. With no sheet or no panel it returns `{}`. It never throws.
- `pdPanelSeedValue_(item, defaults, preserved)` (pure) picks preserved > `_Lists_Config` > built-in. Without a preserved map it is exactly the v1.5.13 expression.

**Builder.** `buildPortfolioDecisionLayout`:
- `off`: no snapshot is taken; seeds come from `pdPanelSeedValue_(item, defaults, null)`.
- `observe`: snapshots, seeds exactly as off, and logs `layout: observe -- N operator input(s) reset to defaults: ...`.
- `enforce`: seeds from the snapshot and logs `layout: preserved N operator input(s) (M differ from defaults: ...)`.

**Totals.** Top-level functions go from 118 to 121: 3 added, 1 changed, 0 removed. The source stays ES5. Added lines are ASCII, and the non-ASCII count is unchanged.

## Measured results

```
node tests/js/pf_panel_preserve.test.js  (x3)  PASS 13/13 digest 40cc928eebcce6f2
PF_BASE_GAS=<v1.5.13> (live base compare)       PASS 13/13 (G2: off == base byte for byte)
golden negative: PF_GAS=<v1.5.13> fails W1 (panel after rebuild = cash 0, Max Position 15)
mutations (no label check, observe applies, off reads, defaults win,
           blank kept, snapshot after clear)    6/6 caught
node tests/js/pf_source_receipt.test.js        PASS 8/8 digest 9cc07d4571cda7ad (unchanged)
node tests/js/pf_ibkr_evidence.test.js         PASS 14/14 digest 7c4bbf7a495aaf79 (unchanged)
node tests/js/apps_script_syntax.test.js       4 sources compiled
pytest tests/test_pf_panel_preserve.py tests/test_pf_source_receipt.py tests/test_pf_ibkr_evidence.py   15 passed
pytest ci.yml lean list (95 files) + the new wrapper   3581 passed, 19 skipped, 0 failed
compileall apps_script scripts core tests       ok
```

**The fixture.** `tests/fixtures/pf_panel_v1_5_13_golden.json` was generated from the v1.5.13 source (`PF_WRITE_GOLDEN=1 PF_GAS=<base>`). It holds:
- the full call trace of a fresh build and of a rebuild over the operator panel;
- the panel after that rebuild;
- the controls the auto-build refresh POSTs;
- a sha256 of the refresh trace.

**Harness cases.**
- WORKS:
  - W1: enforce keeps all 7 typed inputs.
  - W2: blank and dash cells re-seed from `_Lists_Config`, then from the built-in.
  - W3: the auto-build inside `refreshPortfolioDecision` POSTs 53,769.2625 and 20.
  - W4: a fresh workbook seeds exactly as v1.5.13.
  - W5: observe writes exactly as v1.5.13 and logs the 3 reset inputs.
  - W6: case and surrounding blanks are tolerated; the versions are checked.
- NEVER:
  - N1: off and 11 unknown spellings are byte-identical with zero panel reads before the clear; a properties fault reads off.
  - N2: a renamed label or a moved panel keeps nothing.
  - N3: the pure seed table.
  - N4: the snapshot never throws.
  - N5: the added code is ASCII.
- GOLD:
  - G1: the golden shows the reset (0 / 15); enforce differs.
  - G2: the live base compare.

## Install (owner) - paste, nothing is armed by this PR

1. After merge, open the bound Apps Script project and replace the whole of `10_My_Portfolio.gs` with `apps_script/10_My_Portfolio.gs` from main. Save.
2. On Portfolio_Decision, retype the lost inputs once: **B5 = 53769.26** (cash) and **H5 = 20** (Max Position %). Retype B7 only if you want `Advisory` back instead of the default.
3. Proposed arming (owner decision; not done here):
   - Project Settings > Script Properties: `PD_PANEL_PRESERVE = observe`.
   - Run `buildPortfolioDecisionLayout` once (menu or script editor) and confirm the execution log shows `layout: observe -- N operator input(s) reset to defaults: ...` naming the cells you typed.
   - Retype them, set `PD_PANEL_PRESERVE = enforce`, and run the build once more. Expect `layout: preserved 7 operator input(s) (M differ from defaults: ...)` naming at least `PF: Cash Available SAR` and `PF: Max Position %`, with B5 and H5 unchanged.
4. **Rollback:** set `PD_PANEL_PRESERVE = off` (or delete it), or paste v1.5.13.

## Known limits

- Values already lost are not recovered (step 2 retypes them).
- A rebuild that happens while the panel is already on its defaults keeps those defaults; the gate preserves only what is on the sheet before the clear.
