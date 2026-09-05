# Commit sheet — scripts/run_shadow_scorer.py v1.7.1 (PRICE-ERRS read-back, log-only)

**Repo path (exact):** `scripts/run_shadow_scorer.py` — replace the whole file (GitHub web UI → Edit → paste full file → Commit directly to `main`).
**Commit message:** `scorer v1.7.1: [S1-PRICE-ERRS] read-back line (log-only; fetch_spot byte-untouched)`

| | Base (main) | Delivered |
|---|---|---|
| Version | 1.7.0 | **1.7.1** |
| sha256 (first 16) | `e41e0e00dab339fc` | `98272fd87af01edc` |
| Bytes | 80408 | 86998 |
| Source pin | raw.githubusercontent.com == codeload tarball (same SHA) | — |

## What changed (7 anchored edits, each anchor matched exactly once)
1. Header changelog: VERSION 1.7.1 block (why/what).
2. `SCRIPT_VERSION = "1.7.1"`.
3. NEW pure helpers after `fetch_spot`: `_shadow_eodhd_enabled()` (same truthiness as the rescue branch) and `summarize_price_errs(errs, gate_on, token_present)` → (line, JSON-safe dict).
4. `main()`: after `print(verdict)` — computes and prints `[S1-PRICE-ERRS v1.7.1] n=… gate=on|off token=yes|no yahoo_miss=… eodhd_rescued=N/M eodhd_fail=… classes=… first=…` on EVERY path incl. `--dry-run`. Token presence only; the value is never read into the line.
5. S1_Gate meta: the "price errors: N | stale | day" row gains a **4th cell** with the same line (row count/layout unchanged; acceptance regexes `(\d+)/28 scored` and `verdict:` untouched).
6. `_Run_Log` Details JSON: `{"version": "1.7.1", "price_errs": {…}}` (was `{"version": …}`).
7. Selftest: +6 `PE:` checks (miss counting, tag parsing, no-secret line, empty input, derived pool, gate truthiness incl. the "defult" typo → OFF).

**Numerics:** `price errors: N` count, Shadow_History rows/notes, basket math, day classification, `fetch_spot` — **byte-untouched** (AST-proven).
**ENV required:** **none.** No Render change; workflow `shadow_scorer.yml` already maps `TFB_SHADOW_EODHD` and `EODHD_API_KEY`.

## Verification (all on the real repo tree, real module — no stand-ins)
- `py_compile` OK; `python -m compileall main.py core routes scripts` OK.
- AST additive-only proof: 39/39 untouched defs byte-identical; `main`/`_selftest` additions only (no old node altered/removed); 2 defs added; `fetch_spot` AST identical to v1.7.0.
- Selftest baseline v1.7.0: **72/72** → v1.7.1: **78/78** — run ×3 (plain / `TFB_SHADOW_EODHD=1`+token present / `TFB_SHADOW_EODHD=defult`).
- Real-function harness on a 41-entry errs fixture (30 Yahoo HTTP + 5 no_close + 2 no_bar_date + 3 eodhd_fail + tag 32/35): line and dict correct; JSON-safe.
- Lean CI pytest subset (ci.yml list): **232 passed**.

## Pre-existing, NOT introduced, NOT fixed here (register candidate)
`tests/test_shadow_scorer_shape_guard.py` on main expects a v1.6.0 selftest of ≥80 checks with 8 `SG:` markers; main's own v1.7.0 scorer has **72 checks and zero SG markers** → the test fails against main today, before this patch. No workflow runs it (dormant). Adjudicate against the v1.6.0 shape-guard delivery artifact before deciding whether the guard was dropped or the test is stale.

## Post-commit checks (Emad)
1. Open `https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/scripts/run_shadow_scorer.py` — line `SCRIPT_VERSION = "1.7.1"` present; no other new files in the commit.
2. Dispatch `https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/workflows/shadow_scorer.yml` → **Run workflow** → `dry_run = true` (writes nothing).
3. In the run log, step "Score shadow vs champion", find the line starting `[S1-PRICE-ERRS v1.7.1]` and paste it back. Read it as:
   - `gate=on token=yes eodhd_rescued=N/M`, N>0 → rescue fires; then the exclusion cause is elsewhere (benchmark leg / fresh fraction).
   - `gate=on token=no` → the secret is not reaching the run.
   - `gate=off` → the repo Variable is not visible to the run (or unparseable).
   - `eodhd_rescued=0/M` with `eodhd_fail=M` → EODHD rejects the symbols/key; `classes=` names the exception.
4. The scheduled 17:40 Riyadh run then writes the S1_Gate 4th cell and the `_Run_Log` JSON — the artifact-visible read-back from tomorrow's export onward.
