# Commit sheet — RB-TOLERANCE bundle (3 files, ONE commit) — 2026-09-02

Commit all three together (they share one contract: the sync writes `rb_tol=`, the workflow maps the knobs, the acceptance reads the stamp).

| File | Repo path | Base SHA256 (live `main`, f1a273a) | Delivered | Version |
|---|---|---|---|---|
| run_dashboard_sync.py | `scripts/run_dashboard_sync.py` | `8df9054c…` (v6.55.0) | `a2dd4240…` · 10,864 lines | **v6.56.0** |
| daily_sync.yml | `.github/workflows/daily_sync.yml` | (live main) | `ecc72892…` · 1,593 lines | +2 mappings × 2 jobs |
| tfb_acceptance.py | `scripts/tfb_acceptance.py` | (v1.0.4) | `47acc0f7…` · 773 lines | **v1.0.5** |

## What it does
**Problem (measured):** the v6.51.0 cohort verdict stamps any DIVERGENT readback as PARTIAL unless the readback REPAIR restored the prewrite baseline — and that repair is forced-observe for the S-1 window. Residual write-survival after a full sync: GM +6 (guard enforce, 07:00) / +15 (guard off, 11:41), MF +4 / +12, CFX +2. A 0.1–0.2% residual of illiquid foreign names (9984.T, Z74.SI, 7084.KL…) keeps GM PARTIAL, the feed `NOT_ACTIONABLE(partial:GM)`, the board banner ⛔ and every ticket "SIZING WITHHELD" — indefinitely.

**Change (sync v6.56.0):** a DIVERGENT readback whose write-survival delta `rb_flagged − pw_flagged` is within `max(TFB_SYNC_RB_TOL_ROWS, ceil(rb_checked × TFB_SYNC_RB_TOL_PCT/100))` stamps **COMPLETE**, and the stamp message carries `rb_tol=<tol>(+<delta>)` so the residual is never hidden. Readback line, `_Run_Log` evidence and counters unchanged. Missing counters or negative delta → never tolerated.

**Change (acceptance v1.0.5):** D10-5 now measures **write survival (rb − pw)**, not the raw readback count (prewrite-flagged rows are provider incoherence already in the payload). PASS when survived = 0 or the sync's own tolerance decided COMPLETE (`rb_tol=` present); WARN ≤ 1% of rows; FAIL above. Self-test fixtures extended (tolerated stamp → PASS, measured = survived).

## Gates
Both knobs DEFAULT `0` → tolerance 0 → **v6.55.0 byte-identical** (harness A + repo deterministic gate). No arming on commit.
⚑ **Arming (your hands, repo Variables):** `TFB_SYNC_RB_TOL_PCT=0.25`, `TFB_SYNC_RB_TOL_ROWS=2` → GM 17 rows, MF 7, CFX 2, ML 2. Kill-switch: delete both.
With the fill guard armed in enforce, today's counters all stamp COMPLETE (GM +6, MF +4, CFX +2); with the guard OFF, MF (+12 > 7) stays PARTIAL — the tolerance covers the irreducible residual only. **Both are needed.**

## Build protocol
- Live-fetch + SHA: PASS · anchored edits 5 (sync) + 4 (acceptance) + 1×2 (yml), each count-verified · `py_compile`: PASS · YAML parse: PASS, both jobs mapped
- AST zero-removal (sync): 250 → 253, removed 0, added `_rb_tolerance_env`, `_rb_divergence_tolerated`, `_rb_tolerance_note`
- **Repo deterministic gate `scripts/harness_w1a6.py --deterministic` on the patched script: 84/84 PASS**
- Focused harness on the REAL module, original vs patched, today's actual counters (9 cases), ×3: PASS
- `tfb_acceptance.py --selftest`: 6/6 fixtures PASS

## Evidence run
Commit → set the two Variables → confirm `TFB_SYNC_OHLC_FILL_GUARD=1` / `_MODE=enforce` are exactly those strings → dispatch `daily_sync` (full_sync) → expect all four `[STATUS-STAMP]` lines `status=SUCCESS` with `rb_tol=…` on the divergent pages, `[UPSTREAM-VERDICT] EXECUTABLE`, then Top_10 refresh → banner gone, sizing shown → acceptance `D10-4c PASS`, `D10-5-Global_Markets PASS`.

## Addendum — v6.56.1 [ENV-ECHO] (same day, supersedes the v6.56.0 script file)
**Delivered:** `scripts/run_dashboard_sync.py` v6.56.1 · SHA256 `c15460525436cb63…` · 10,902 lines.
**Evidence:** run 33620619118 (14:03) ran v6.56.0 with a ZERO tolerance (MF rb−pw=+1 stamped PARTIAL; tolerance would have been 7) and the fill guard OFF (no `[OHLC-FILLGUARD]` line) — while the yml on `main` maps all four Variables and the scheduled 07:00 run logged `mode=enforce`. Nothing in the log could say whether a Variable reached the job.
**Change (log-only):** one INFO line at self-test time, e.g. `[ENV-ECHO v6.56.1] status_truth=True … fill_guard=True('1') fill_mode=enforce('enforce') rb_tol_rows='2' rb_tol_pct='0.25' rb_tol@6609=17`. A Variable that never reached env shows as `unset`; a value the reader rejects shows as e.g. `fill_guard=False('enforce')`.
**Protocol:** anchored edits 4 (count==1), py_compile, AST zero-removal (added `_env_echo_line`), smoke on the real module (unset / armed / operator-slip), repo deterministic gate 84/84 PASS.
**Register item (not changed today):** CFX readback `UNAVAILABLE (read_failed)` stamped SUCCESS — the v6.51.0 rule only turns DIVERGENT into PARTIAL; an UNKNOWN readback passes. Latent false-green on transient read failures.
