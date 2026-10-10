# scripts/harness_archive

Historical, one-shot build harnesses. **Nothing here is executed by any
workflow, and nothing here should be run as-is.**

## Why this directory exists

These 21 files were uploaded through the GitHub web UI with mangled names: a
space instead of the first separator and a U+00B7 MIDDLE DOT instead of the
extension dot, e.g.

```
scripts/Harness ob1190·py
scripts/Harness valuation firewall v5135·py
scripts/Harness dt1110·JS
```

Because none of them ended in `.py` or `.js`, they were invisible to every
tool in the repository: `python -m compileall ... scripts` skipped them,
pytest never collected them, and no editor or grep-by-extension found them.
They were renamed on 2026-10-06 to `harness_<name>.py` / `.js` and moved here
so the tree is searchable and the files are syntax-checked by CI like any
other source file.

They are **archived rather than deleted** because ten TFB Commit Sheets in
`docs/evidence/` cite them as the proof battery for a delivered build. The
mapping is mechanical: `Harness ob1190·py` -> `harness_ob1190.py`.

## Why they cannot be run unchanged

15 of the 21 hard-code the sandbox paths of the session that produced them,
for example:

```python
sys.path.insert(0, "/home/claude/repo/tadawul-fast-main")
U = "/mnt/user-data/uploads/_Market_Share_Deepseek-V3_-_%s.tsv"
fs.readFileSync('/home/claude/dt_new.js')
```

and several expect a "base" copy of the pre-change module, or a workbook
export TSV, that was never committed. They are evidence of a past audit, not
a reusable suite.

## What to use instead

The live, runnable batteries are in `tests/`. A new build's harness belongs
there, as a file that works both as `python tests/<name>.py` and under
`python -m pytest -q tests/<name>.py`, with no absolute paths and no network.

## Added 2026-10-10: dual-tree test harnesses moved out of `tests/`

A project-wide review found that `python -m pytest tests` could not run at
all: one module exited the interpreter during collection, and eighteen
others either loaded a "base" copy of a module that was never committed or
pinned an exact module version that later releases had moved past. None of
them is referenced by any workflow in `.github/workflows/`. They are kept
here, unchanged, because the Commit Sheets in `docs/evidence/` cite them.

| File | Why it cannot run from the repository |
| --- | --- |
| `test_board_engine_roi_p139.py` | needs `board.py` / `board_v140.py` (shadow-board base and fix copies); also replaces `sys.modules["core"]` with a stub at import, which broke every later module's collection |
| `test_harness_loopguard.py` | needs `eodhd_provider_v4170_ORIGINAL.py`; monkeypatches `httpx.AsyncClient.get` process-wide at import |
| `test_pf_f1_plan_basis_dualtree.py` | needs `tests/portfolio_actions_base.py` and `tests/portfolio_actions.py` (v1.11.1 vs v1.12.0 copies) |
| `test_ob_ann_roi_annualized.py` | needs `ob_b/` and `ob_r/` trees (opportunity_builder 1.19.5 vs 1.19.6) |
| `test_pa_position_qty_alias.py` | needs `qb/` and `qr/` trees (portfolio_actions 1.11.0 vs 1.11.1) |
| `test_scoring_roi_parse.py` | needs `sb/` and `sr/` trees (scoring 5.11.1 vs 5.11.2) |
| `test_ob_f1b_plan_basis.py` | pins opportunity_builder 1.20.0; fixtures dated 2026-09 now fail the builder's data-trust freshness gate |
| `test_ob_price_xcheck.py` | pins the 1.21.0 behaviour; fixtures predate the reconciled-position / settled-cash requirement, so no seat funds |
| `test_ob_nearmiss_text_p171.py` | pins opportunity_builder 1.23.2; same funding-input change as above |
| `test_ob_w52_timing_p181.py` | pins opportunity_builder 1.23.2; same |
| `test_sync_retire_tab_p174.py` | pins run_dashboard_sync 6.64.2; the Details JSON contract it checks changed in 6.64.x |
| `test_sb_atomic_write.py` | pins run_shadow_board 1.3.1 and run_shadow_scorer 1.8.0 |
| `test_idg_schema_shift.py` | takes the repo tree from `sys.argv[1]` (pytest passes its own arguments) and reads `/mnt/user-data/uploads/*.tsv` exports that were never committed |
| `test_ledger_totals_p135.js` | needs `ledger_base.gs` / `ledger_v150.gs` (21_Portfolio_Ledger is not in the repository) |
| `test_gas_trade_notes_v100.js` | needs `apps_script/25_Trade_Notes.gs`, which is not in the repository |
| `test_dt10_p144_epoch_key.js` | golden-negative needs `16_Decision_Top10_base.gs` |
| `test_dt10_p168_outage_pause.js` | needs `16_Decision_Top10_base.gs` and an `../exp` TSV export |
| `test_dt10_v11112_cockpit_truth.js` | needs `16_Decision_Top10_base.gs` |
| `test_dt10_v11113_prop_memo.js` | needs `16_Decision_Top10_base.gs`; its fixture stays at `tests/fixtures/dt10_pages_2026-10-05.json` |

The live Node harnesses that remain in `tests/` (`test_dt10_cash_source.js`,
`test_dt10_p142_containment.js`, `test_dt10_p145_grace_sizing.js`) now resolve
`apps_script/16_Decision_Top10.gs` relative to the test file, so they run from
any working directory; `test_dt10_p142_containment.js` also accepts LF line
endings (it previously required CRLF).
