TFB Commit Sheet — scripts/run_dashboard_sync.py v6.70.0 [FW-SKEW: classify P/E-coherence quarantines (C2)]
Date: 2026-10-16 (Friday, evidence day) · Lane: GitHub/Python (daily_sync.yml, both writer lanes) · File status: OLD file, rebuilt · Nothing armed by this delivery

S0/S1 — Base pin
Item	Value
Base	`scripts/run_dashboard_sync.py` v6.69.0 at `origin/main` 11ef134
Delivered	sha `531f745fce4616ff…`, 15,087 lines, `SCRIPT_VERSION = "6.70.0"` (banner re-pinned)
Edits	9 anchored edits, count==1 each (banner · version + WHY · 4 helpers after the 52W aliases · firewall signature · firewall docstring · 52W column lookup · per-row classification · call site (off = the exact v6.69.0 two-argument call) · header count)
AST	functions 349 → 357 (+4 top-level `_fw_skew_mode`, `_fw_skew_max_rel`, `_fw_skew_explained`, `_fw_skew_line`; +4 nested formatters; 0 removed) · ruff E9/F63/F7/F82 clean

S2 — Why (GitHub Actions logs; diagnosis-first per the 10-16 brief)
7 Global_Markets passes (10-14 09:37Z … 10-16 00:51Z): every one of 268 backend calls per pass returned 200; `f429=0 f404=0 fetch_failed=0 rows402 new=0`; EODHD peaked at 32.4% of quota. The throttling hypothesis is rejected (the run took 1h48m, not 4h44m: 03:54 was Riyadh time).
fresh_rows = 6412 − FW-2 strips in every pass: strips 128 / 187 / 70 / 359 / 270 / 285 / 252 against a 95% floor that tolerates ≤133. The 449 invalid rows on 10-16 = 252 FW-2 strips (restored from last-good → `identity_quarantined:kept_last_good` → not fresh) + a fixed 197 (never fresh in any pass).
FW-2 strips a row when |Price/EPS − P/E| / P/E ≥ 5% (`_COH_REL_TOL`, whose own comment names "as-of skew between the quote and the fundamentals snapshot"). The same names recur (IBO.US, PRIM.US, FORM.US, NNNN.US, 006800.KS in 5 of 7 samples) and counts are higher in the US session (252–359 vs 70–187) — the shape of a live price against a P/E computed at an older snapshot. With zero strips the 10-16 main leg reads 97.0%.

S3 — What
`TFB_SYNC_FW_SKEW_AWARE` = off (default, byte-identical) | observe | enforce; `TFB_SYNC_FW_SKEW_MAX_REL` (default 0.35, clamp 0.05..1.0).
For every row FW-2 would strip, pure `_fw_skew_explained()`: P_snap = EPS × P/E (the price the stated P/E was computed at). Explained iff P_snap and today's price both lie inside the row's OWN 52-week range (±2%) and |px/P_snap − 1| ≤ max_rel. A row whose fundamentals belong to another instrument almost never satisfies both. Reasons: explained | no_52w | range_invalid | px_outside_52w | snap_outside_52w | rel_over_cap.
observe: one `[FW-SKEW v6.70.0] page=.. mode=observe would_strip=N explained=K kept=0 reasons=.. rel_explained=.. rel_unexplained=.. suffix_explained=.. sample_explained=[..] sample_unexplained=[..]` line per page (res.warnings + log; no braces — the Actions log masks them). Rows untouched.
enforce: explained rows keep their freshly fetched cells (no strip, no last-good restore); unexplained rows are stripped and restored exactly as before.
UNTOUCHED: the 5% tolerance, the 50–200 GBX band, the L3b page tripwire, FW-5, FW-KEEP, every floor and freshness threshold. With the mode off, the firewall is called with the exact v6.69.0 two-argument signature.
Workflow: `daily_sync.yml` maps both Variables in sync-dashboard AND recover-missing-market-pages (`${{ vars.X || default }}`, inert); `tests/test_workflow_flag_mappings.py` SYNC_DEFAULTS +2 (parity enforced).

S4 — Audits
Battery	Result
New `tests/test_fw_skew_c2.py` (22)	classifier table 8/8 · mode parsing 6/6 · max_rel clamp 5/5 · no-skew path == v6.69.0 path · observe/enforce classification + line format (no braces) · REAL runner (`_run_one_task`, fake backend + writer, Global_Markets): off → both mismatches restored from last-good, no line · observe → published rows identical to off + one line `would_strip=2 explained=1 kept=0` · enforce → HOOD.US keeps price 120 and no quarantine tag, IBO.US (snapshot price 15 outside its range) restored as before, page fresh count +1 — 22 passed ×3
Mutation	enforce never keeps → 2 failed; snapshot-range check removed → 2 failed
Regression	every test naming run_dashboard_sync / daily_sync.yml + manifest pins: 1,526 passed (GITHUB_EVENT_NAME unset) / 1,523 passed (pull_request); the first build passed a 3rd argument in off mode and broke `test_sync_sheet_presentation` (a 2-arg firewall stub) — fixed by keeping the exact v6.69.0 call when off
Workflow audit	`audit_repository_workflows.py` errors=0 (2 pre-existing warnings)
Manifest	`verify_deployment.py` pin run_dashboard_sync 6.69.0 → 6.70.0 (pin line only)

S5 — Delivery
`scripts/run_dashboard_sync.py` · `.github/workflows/daily_sync.yml` · `scripts/verify_deployment.py` (pin) · `tests/test_fw_skew_c2.py` (NEW) · `tests/test_workflow_flag_mappings.py` · this sheet
Merge note: #783 also bumps run_dashboard_sync (6.69.1) — whichever merges second takes the next version on the banner, SCRIPT_VERSION and the pin.

S6 — Arming / read-back (owner, GitHub Variables; NOT done by this delivery)
Step 1 (decision-neutral): `TFB_SYNC_FW_SKEW_AWARE=observe`. Read-back on the next GM pass: `[FW-SKEW v6.70.0] page=Global_Markets mode=observe would_strip=N explained=K …`. Decision rule: if K/N is high and `sample_unexplained` lists the genuinely broken names, enforce is safe; if most strips are `snap_outside_52w`, they are real identity/currency problems and enforce would not help.
Step 2 (evidence-lane sitting): `enforce`. Read-back: `kept=K` and GM `PAGE-VERDICT fresh_pct` up by ≈K/6609.
Rollback: delete the Variable. A tighter `TFB_SYNC_FW_SKEW_MAX_REL` narrows enforce without code.
