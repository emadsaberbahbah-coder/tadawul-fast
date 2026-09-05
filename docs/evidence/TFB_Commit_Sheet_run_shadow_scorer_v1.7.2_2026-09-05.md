# Commit sheet — scripts/run_shadow_scorer.py v1.7.2 (SHAPE GUARD + EXCLUSION REASON · P-71 / P-79) — 2026-09-05

| File | Repo path | Exists? | Delivered | Version |
|---|---|---|---|---|
| run_shadow_scorer.py | `scripts/run_shadow_scorer.py` | replace | SHA256 `fcaaa710feea1ee5…` · 95,910 bytes · 1,887 lines | v1.7.1 (`98272fd87af01edc`) → **v1.7.2** |

Repo copy of this sheet (optional, new): `docs/evidence/TFB_Commit_Sheet_run_shadow_scorer_v1.7.2_2026-09-05.md`.
**Commit message:** `scorer v1.7.2: shape guard (meta rows never priced) + exclusion reason; revives test_shadow_scorer_shape_guard (P-71, P-79)`

## Evidence — run #52 (dry-run, the first v1.7.1 read-back)
`[S1-PRICE-ERRS v1.7.1] n=38 gate=on token=yes yahoo_miss=30 eodhd_rescued=0/30 eodhd_fail=8 …` with the failing "symbols" = `null`, `OK`, `NO_ACTION`, `6`, `8.06`, `SHADOW BOARD v1.3.0` and JSON fragments of the regime stamp.
- **The 09-03 EODHD arming is real** (gate on, token present, 19 EODHD calls made).
- **None of the 19 misses is a symbol.** They are Shadow_Board META rows admitted as data rows → swept into the W-7 EQW basket ("n=13" = 6 names + 7 meta strings) → written to Shadow_History → carried back into `need` daily. 19 junk × (Yahoo + EODHD) = the 38 "price errors".
- **CHALLENGER symbols = ''** in the same run: the shadow board has published 0 eligible names most days (BROKER_UNTRADABLE / MODEL_SCREEN_FAIL) → fresh coverage 0/0 → day excluded; the note blamed infra.
- The dormant `tests/test_shadow_scorer_shape_guard.py` (v1.6.0, ≥80 checks + 8 SG markers) described exactly this guard and never reached the script — it failed against main (72 checks, no SG).

## What changed (14 anchored edits, each matched exactly once)
1. `import re`; `_SYMBOL_SHAPE_RE`; new pure `_shape_guard_enabled()`, `symbol_like()`, `shape_guard_rows()`.
2. `main()`: board data rows pass the guard (first cell must be symbol-shaped); previous-row `symbols`/`prices` from Shadow_History are filtered (on copies); `need` is filtered before `fetch_spot`. S-1 baskets (CHAMPION / CHALLENGER / BENCHMARK) contain only real symbols → **unmoved**; EQW (informational) becomes real-names-only.
3. Exclusion reason: `reason=no-challenger | fresh-floor | benchmark-leg` appended to the Shadow_History note (prefix `DAY_EXCLUDED_INFRA` untouched → `count_scored_days` / retro counters byte-identical), to the verdict line (`| excluded_reason=…`), to the S1_Gate meta (new 5th cell with the guard line) and to the `_Run_Log` JSON (`shape_guard`, `excluded_reason`).
4. `[S1-SHAPE-GUARD v1.7.2] on dropped=N board_rows=… need=… first=[…]` printed after the price-errs line (read-back).
5. `summarize_price_errs`: class = LAST colon segment (the only non-additive line; JSON fragments had garbled the histogram).
6. Selftest: +8 `SG:` checks carrying the dormant test's exact markers, +1 `PE:` check → **87/87**.
**Kill switch:** `TFB_SHADOW_SHAPE_GUARD=0` → v1.7.1 lists byte-for-byte. **ENV required: none** (default ON).

## Verification (real repo tree, real module)
- py_compile / compileall OK · AST: 40/43 defs byte-identical; `main` + `_selftest` additive-only; `summarize_price_errs` exactly the rpartition change; 3 defs added; 0 removed.
- Selftest v1.7.1 78/78 → v1.7.2 **87/87 ×3** (plain / `TFB_SHADOW_SHAPE_GUARD=0` / EODHD env).
- `tests/test_shadow_scorer_shape_guard.py`: **FAIL on main → PASS** (all 8 markers present, ≥80 checks).
- Replay of run #52's 19 junk tokens + 14 real symbols: `need` 33 → 15; real symbols dropped: **none**; junk surviving: `OK` only (a 2-letter uppercase token; it leaves with the next clean history write). Histogram now `HTTPStatusError:19, eodhd_HTTPStatusError:19`.
- Lean CI (ci.yml list + the revived test): **238 passed**.

## Post-commit checks (Emad)
1. `SCRIPT_VERSION = "1.7.2"` on main; no unexpected files.
2. Dispatch `shadow_scorer.yml` with `dry_run=true` → expect `[S1-SHAPE-GUARD v1.7.2] on dropped=7 …` and `[S1-PRICE-ERRS v1.7.2] n≤2 …` (junk gone; `OK` may linger once), and `excluded_reason=no-challenger` on the verdict line.
3. Tomorrow's scheduled 17:40 run writes the first clean EQW row; the day after, `n=0` is the expected read.

## What this does NOT fix (register, P-79)
S-1 cannot accrue evidence while the shadow board seats 0 eligible names: the challenger is empty, so every trading day is excluded by construction. That is a challenger-scope / compliance-screen decision (align the shadow board's tradable universe with `TFB_T10_VENUE_ALLOWLIST` / `DERAYAH_MARKETS`), not a scorer defect. Until it is decided, "3/28 scored" does not move.
