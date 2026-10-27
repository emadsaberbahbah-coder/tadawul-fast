# TFB Commit Sheet - 10_My_Portfolio.gs v1.5.14 [PANEL INPUTS SURVIVE A REBUILD]

Renumbered 2026-10-21: main shipped a different v1.5.14 first (#810, the ledger reader excludes sold/closed lots). This build is now **v1.5.15** on top of it: off is byte-identical to that v1.5.14 (the harness's live base compare passes with `PF_BASE_GAS=<main v1.5.14>` as well as with the v1.5.13 golden source), and the golden negative still fails W1 on main's v1.5.14 (panel after rebuild = cash 0, Max Position 15). The harness digest is now `4af915fbb6e6da76` (the version string is part of the traced log lines). The figures below are as measured on 2026-10-15 against v1.5.13.

Renumbered again 2026-10-25: main shipped a different v1.5.15 first (#834, PD-ALLCASH: a broker-proven all-cash book publishes status ok and the readiness receipt). This build is now **v1.5.16** on top of it. Off is byte-identical to main's v1.5.15 on the panel path: the live base compare passes with `PF_BASE_GAS=<main v1.5.15>` (16/16). The golden negative still fails W1 on main's v1.5.15 (panel after rebuild = cash 0, Max Position 15). The harness digest is now `f8b85742e2e61dc6`. Main's #834 harnesses were adjusted for this build's additions only (the Logger-line check reads `PD_VERSION`; the inventory counts the three panel-preserve functions and 141 lines), and they pass: `pf_allcash_ok` 7/7, `tests/test_pf_allcash_ok.py` with `TFB_PF_BASE_REF=4123c831^`. The paste is now **v1.5.16** from main; it contains #834's v1.5.15 and #810's v1.5.14.

Renumbered again 2026-10-27: main shipped a different v1.5.16 first (#844, EVIDENCE PUBLICATION: the SENT IBKR packet is published as the Top_10 cockpit's funding evidence, PD_IBKR_EVIDENCE_PUBLISH, default off). This build is now **v1.5.17** on top of it. Off is byte-identical to main's v1.5.16 on the panel path: the live base compare passes with `PF_BASE_GAS=<main v1.5.16>` (16/16), and the golden negative still fails W1 on main's v1.5.16 (panel after rebuild = cash 0, Max Position 15). The harness digest is now `4a015c0b9fb8a6b3`. Main's #844 harnesses were adjusted for this build's additions only: `tests/test_pd_evidence_publish.js` P8 takes a v1.5.16 floor with the header line in lockstep, P9 (with `PD_BASE=<main v1.5.15>`) counts the three panel-preserve functions (61 checks pass); the `tests/test_pf_allcash_ok.py` inventory lists them and counts 142 lines; the ledger harness takes a PD floor of 1.5.16; `pf_allcash_ok` already reads `PD_VERSION` (main's `ctxVersion()`), 7/7. Known gap: with `TFB_PF_BASE_REF=4123c831^` the v1.5.14-base diff test now fails on main's own #844 lines (main alone: 328 added / 12 removed against bounds of 95-110 / 10; the test skips without that ref, so CI never ran it); this build's share (145 added / 3 removed) is counted. The paste is now **v1.5.17** from main; it contains #844's v1.5.16, #834's v1.5.15 and #810's v1.5.14.

Date: 2026-10-15 (daily improvement, one item).
- **Base:** v1.5.13 (main `11ef1345`, sha256 `a8a00d6d...`, 4,529 lines). Result sha256 `63f840c9...`, 4,667 lines.
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

Anchored edits on the pinned base, each asserted `count == 1`:
1. Header version, plus a new v1.5.14 WHY block above v1.5.13 (all prior WHY blocks verbatim).
2. `PD_VERSION` is now `'1.5.14'`.
3. The builder and three new functions, described below.

**New functions.**
- `pdPanelPreserveMode_()` reads the Script Property the same way as `PD_SOURCE_RECEIPT` (trim, lower case; a fault reads off).
- `pdPanelPreserve_(sheet, faults)` snapshots the panel's NON-BLANK values before the clear. A value is kept only when its label cell still holds the exact `PF:` label at the same position, so a moved or renamed layout keeps nothing. With no sheet or no panel it returns `{}`. It never throws: a read fault is pushed onto the optional `faults` array, so the caller can tell a failed read from an empty panel.
- `pdPanelSeedValue_(item, defaults, preserved)` (pure) picks preserved > `_Lists_Config` > built-in. Without a preserved map it is exactly the v1.5.13 expression.

**Builder.** `buildPortfolioDecisionLayout`:
- `off`: no snapshot is taken; seeds come from `pdPanelSeedValue_(item, defaults, null)`.
- `observe`:
  - snapshots, seeds exactly as off, and logs `layout: observe -- N operator input(s) reset to defaults: ...`;
  - on a snapshot read fault it logs `layout: panel snapshot failed (...); observe continues with the defaults`.
- `enforce`:
  - seeds from the snapshot and logs `layout: preserved N operator input(s) (M differ from defaults: ...)`;
  - on a snapshot read fault (Codex P2 on this PR) it logs and throws `layout: panel snapshot failed (...); rebuild aborted, sheet left untouched. Retry, or set PD_PANEL_PRESERVE=off to rebuild with the defaults.`;
  - the throw happens BEFORE `sheet.clear()`. A transient Sheets failure therefore cannot become the reset this mode exists to stop.
  - On the auto-build path the refresh fails visibly instead of sizing on the defaults.

**Totals.** Top-level functions go from 118 to 121: 3 added, 1 changed, 0 removed. The source stays ES5. Added lines are ASCII, and the non-ASCII count is unchanged (296 bytes, all pre-existing).

## Measured results

```
node tests/js/pf_panel_preserve.test.js  (x3)  PASS 16/16 digest 055bf09bffe9433f
PF_BASE_GAS=<v1.5.13> (live base compare)       PASS 16/16 (G2: off == base byte for byte)
golden negative: PF_GAS=<v1.5.13> fails W1 (panel after rebuild = cash 0, Max Position 15)
mutations 10/10 caught: no label check, observe applies, off reads, defaults win,
  blank kept, _Lists_Config dropped at the call site, kept cells skip the number
  format, no abort on a fault, faults not recorded, abort in observe
node tests/js/pf_source_receipt.test.js        PASS 8/8 digest 9cc07d4571cda7ad (unchanged)
node tests/js/pf_ibkr_evidence.test.js         PASS 14/14 digest 7c4bbf7a495aaf79 (unchanged)
node tests/js/apps_script_syntax.test.js       4 sources compiled
pytest tests/test_pf_panel_preserve.py tests/test_pf_source_receipt.py tests/test_pf_ibkr_evidence.py   15 passed
pytest ci.yml lean list (95 files) + the new wrapper   3581 passed, 19 skipped, 0 failed
ci.yml node steps (7 harnesses) + both PD harnesses  all pass
compileall apps_script scripts core tests       ok
```

**The fixture.** `tests/fixtures/pf_panel_v1_5_13_golden.json` was generated from the v1.5.13 source (`PF_WRITE_GOLDEN=1 PF_GAS=<base>`). It holds:
- the full call trace of a fresh build and of a rebuild over the operator panel;
- the panel after that rebuild;
- the controls the auto-build refresh POSTs;
- a sha256 of the refresh trace.

The test `_Lists_Config` deliberately differs from the built-in defaults for Max Sector (25 vs 30) and Min Reliability (65 vs 70), so a seed that skips `_Lists_Config` is caught.

**Harness cases.**
- WORKS:
  - W1: enforce keeps all 7 typed inputs, and its full write trace equals v1.5.13's with only the 7 seed values swapped (formats and colours pinned).
  - W2: blank and dash cells re-seed from `_Lists_Config`, then from the built-in (trace pinned the same way).
  - W3: the auto-build inside `refreshPortfolioDecision` POSTs 53,769.2625 and 20.
  - W4: a fresh workbook seeds exactly as v1.5.13 (`_Lists_Config` > built-in).
  - W5: observe writes exactly as v1.5.13 and logs the 5 reset inputs.
  - W6: case and surrounding blanks are tolerated; the versions are checked.
  - W7: an observe snapshot fault is logged, and the build carries on as v1.5.13.
- NEVER:
  - N1: off and 11 unknown spellings are byte-identical with zero panel reads before the clear; a properties fault reads off.
  - N2: a renamed label or a moved panel keeps nothing.
  - N3: the pure seed table.
  - N4: the snapshot never throws, and faults are recorded only on a fault.
  - N5: the added code is ASCII.
  - N6: an enforce snapshot fault aborts before the clear. There are no writes, the panel is untouched, and on the refresh path nothing is POSTed.
  - N7: off ignores read faults (no snapshot) and stays byte-identical.
- GOLD:
  - G1: the golden shows the reset (0 / 15); enforce differs.
  - G2: the live base compare.

## Install (owner) - paste, nothing is armed by this PR

1. After merge, open the bound Apps Script project and replace the whole of `10_My_Portfolio.gs` with `apps_script/10_My_Portfolio.gs` from main. Save.
2. Proposed arming (owner decision; not done here): Project Settings > Script Properties, `PD_PANEL_PRESERVE = enforce`.
3. On Portfolio_Decision, type your CURRENT values once:
   - B5: available cash in SAR (the 10-14 snapshot read 53,769.26; use today's figure, for example the latest `_Cash_Snapshot` or IBKR balance);
   - H5: Max Position % (20).
4. Optional check: run `buildPortfolioDecisionLayout` (menu or script editor). The execution log should read `layout: preserved 7 operator input(s) (...)`, with B5 and H5 unchanged.
   - Doing step 2 before step 3 avoids a window in which a rebuild re-seeds the defaults over the values just typed.
   - `observe` is available for a dry run before arming. It seeds the defaults like off, so retype B5 and H5 after an observe rebuild.
5. **Rollback:** set `PD_PANEL_PRESERVE = off` (or delete it), or paste v1.5.13.

## Known limits

- Values already lost are not recovered; step 3 retypes them.
- Enforce keeps every non-blank cell, stale or not. This is the P-134 lesson from 16_Decision_Top10 v1.11.7. Keep B5 current. To apply a changed `_Lists_Config` default, or to reset a value, blank the cell first (or rebuild once with `off`). A rebuild while the panel already holds the defaults keeps those defaults.
- A formula in a panel cell is kept as its value (the snapshot reads `getValue`).
- A moved panel (a row or column inserted above or left of it) keeps nothing, as in the Top_10 reference. The log then reads `preserved 0 operator input(s)`.
- The node harness runs in CI's whole-suite job, which is advisory (continue-on-error, outside the CI verdict), like the other 10_My_Portfolio harnesses. Moving all three into the blocking lean lane is proposed in the PR. This PR does not touch `ci.yml`, to stay clear of #778.
