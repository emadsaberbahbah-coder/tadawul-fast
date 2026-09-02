# Commit sheet — core/analysis/opportunity_builder.py v1.19.1 [AUDIT-DEPTH-ORDER]

**Base (live-fetched from `main`, 2026-09-02):** v1.19.0 · SHA256 `4340cfe38a470a575fdb10e6c38788b8612886193fd5bca708c3295aa0dfb7bb`
**Delivered:** v1.19.1 · SHA256 `44721dd60687d16c2adb8a76198996c56a099ce0cfd56d0a33e6645e352410c7` · 5,147 lines (was 5,076) — second-pass review applied (GATE_ORDER index hoisted to module level; behaviour identical)
**Repo path:** `core/analysis/opportunity_builder.py` (replace whole file via GitHub web UI)

## What it changes (display-only)
The written audit grid and the NEAR MISS list are ordered by **how far a row got through the gate chain**
(INVEST first → deepest first-fail gate → reliability desc → score desc → symbol), not by opportunity score.
Applied at three sites: the audit pre-sort, the cap fill (`TFB_OPP_AUDIT_ROWS_MAX`), and the near-miss pool.
**Selection, tickets, KPIs, alerts, funding states: byte-identical** (harness-proven).

## Kill-switch
`TFB_OPP_AUDIT_ORDER=score` → v1.19.0 ordering byte-for-byte (harness scenario B). Default `depth` — no ENV needed to arm.
Render reads env directly; no workflow mapping required (builder runs on the web service, not in Actions).

## Evidence of the defect it fixes
Harness scenario A (real `build_opportunity_payload`, 20-row pool, cap 6): v1.19.0 wrote **0** of 3 deep-fail rows;
v1.19.1 wrote all 3 with `first_fail = Risk Level`. Production: 11 of 14 sane candidates absent from every board since 08-31.

## Build protocol
- Live-fetch + SHA verify: PASS (tarball `main`, base SHA above)
- Anchored edits: 9 (7 build + 2 review), each `count==1`: PASS
- `py_compile`: PASS
- AST zero-removal: 136 → 138 functions, removed 0, added `_env_audit_order`, `_audit_depth_key`: PASS
- Harness on REAL module (original vs patched), 3 scenarios × 3 runs: PASS ×3
- Repo suite `tests/test_opportunity_builder.py` + `tests/test_top10_selector.py`: **42 passed**

## Deploy
Commit → Render Manual Deploy (Auto-Deploy is OFF) → GAS Top_10 refresh → export. Expect: near-miss shows real gates
(Risk/Reward, Forecast Cap Band, …) instead of ten Valuation Sanity rows; the 11 missing candidates appear in the audit.

## Second-pass review (2026-09-02, pre-apply) — findings
1. Audit order is consumed only by the written grid and near-miss list; `_build_alerts` counts by first_fail (order-free);
   selection/tickets/KPIs are computed before the sort. Harness A/B/C byte-identical on selection surfaces: re-confirmed after the hoist.
2. Qualified-but-unfunded rows now lead NEAR MISS as "Funding" rows (they also appear in ALL QUALIFIED). Intentional; the truest near-miss.
3. `TFB_OPP_AUDIT_ROWS_MAX=300` on the instance stays as is; depth order makes the 300 written rows the *deepest* 300.
4. The regret benchmark (`ALT_TOP10`) string is not present in any `.py` on `main`; assumed to read `_Selection_Log`, not the audit order. Low risk, unverified.
