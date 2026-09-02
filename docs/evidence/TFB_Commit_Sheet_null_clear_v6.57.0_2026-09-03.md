# Commit sheet — NULL-CLEAR SCOPE bundle (2 files, ONE commit) — 2026-09-03

| File | Repo path | Exists? | Delivered | Version |
|---|---|---|---|---|
| run_dashboard_sync.py | `scripts/run_dashboard_sync.py` | replace | SHA256 `a64602671dc52e06…` · 10,986 lines | v6.56.2 → **v6.57.0** |
| daily_sync.yml | `.github/workflows/daily_sync.yml` | replace | +2 mappings × 2 jobs | — |

Project copy of this sheet: `TFB_Commit_Sheet_null_clear_v6.57.0_2026-09-03.md` · repo copy: `docs/evidence/` same name (new).

## The root cause (measured on the 2026-09-03 00:40 export vs engine v5.135.0)
- MCHPP.US: the engine's own `Upside/Downside %` = −0.001 → its target ≈ price at the boundary; the sheet cell reads **218.364**. NBRG.US: engine upside 0.001 (target ≈ 10.08); cell **1,642.86**. 14 rows carry the engine's `*_rejected_outlier` tag (target emitted as null) and still show a value.
- The values differ from the previous day (MCHPP 129,500 → 218.364): not this symbol's stale cell.
- Mechanism, written in the script's own v6.44.0 note and now measured on non-OHLC columns: **`values.update` skips JSON-null cells; the page is written-then-trimmed with no pre-clear; rows re-sort between runs — so a null Target Price inherits whatever symbol sat in that row position on the previous write.** Over weeks of daily re-sorts nearly every position accumulates a stale value: 6,080/6,609 GM rows show a Target Price, 89.9% disagree with the row's own Upside %, 40% are outside [0.25×, 3.0×]. The same grafting explains P/E ≠ Price/EPS (47.6%), wrong sectors (KLAC.US "Basic Materials"), and the Top_10 Valuation Sanity carnage. The v6.44 guard deliberately left non-OHLC columns on null-skip ("keep-last-good for provider targets") — that keep-last-good is exactly the graft.

## The fix
`TFB_SYNC_NULL_CLEAR_SCOPE=all` → when the fill guard is armed, **every** header except `Symbol` (and `TFB_SYNC_NULL_KEEP_COLS`, CSV, default empty) is guarded: observe counts, enforce writes `""` so an honest blank replaces the graft. Default `ohlc` → v6.56.2 byte-identical (harness A). Engine-side keep-last-good for targets (`engine_target_klg=true`) and the row-level persistence paths (SYMBOL-PERSISTENCE, FW-KEEP) are untouched.

## What you will see on the first enforce run (expected, not a regression)
A large one-time blanking: Target Price disappears from most synthetic rows (only real provider targets + sane engine targets remain), stale P/E / EPS / sector cells go blank, `missing_valuation` on the board rises to ~5,000, coherence B1/B2/B3 drop sharply, Upside % stays (it is engine-computed and never null).

## Protocol
Live-fetch + SHA · 6 anchored edits (count==1) · py_compile · AST zero-removal (added `_null_clear_scope`, `_null_clear_keep_cols`, `_null_clear_all_cols`) · harness on the REAL `_ohlc_fill_guard_apply`, original vs patched, 4 scenarios × 3: PASS · repo deterministic gate `harness_w1a6.py --deterministic` **84/84 PASS** · YAML parsed, both jobs mapped.

## Arming (your hands, repo Variables) — sequence
1. Commit both files. 2. `TFB_SYNC_NULL_CLEAR_SCOPE=all` (the guard is already `1`/`enforce`). 3. Dispatch `daily_sync` full_sync. 4. ENV-ECHO unchanged; the `[OHLC-FILLGUARD]` `_Run_Log` lines now read `scope=all | total=<cleared>`. 5. Tomorrow's export: coherence B1/B2 measured against today's baseline.
Kill-switch: delete `TFB_SYNC_NULL_CLEAR_SCOPE`.
