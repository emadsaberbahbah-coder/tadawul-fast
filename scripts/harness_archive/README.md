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
