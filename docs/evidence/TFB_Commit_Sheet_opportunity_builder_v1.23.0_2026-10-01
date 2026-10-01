TFB Commit Sheet — core/analysis/opportunity_builder.py v1.23.0 [P-181 TIMING GATE: two-sided 52W window + one-session shock veto]
Date: 2026-10-01 (Thursday) · Lane: Render/Python · Build #1 of the day · Protocol: One-Pass · File status: OLD file, rebuilt (full-file delivery)
S0/S1 — Base pin
Item	Value
Base	`core/analysis/opportunity_builder.py` v1.22.2 at HEAD `7575c7d` (sha `d53795d10e465866…`, 6,420 lines) — live on Render per `/health` 06:06Z (`opportunity_builder_version 1.22.2`)
Delivered	sha `0f3ce13c61466dfe…`, 6,665 lines, `OPPORTUNITY_BUILDER_VERSION = "1.23.0"`
Edits	10 anchored edits, count==1 each (header+version · field aliases · env readers + helpers · cand fields when armed · evaluate_gates append · GATE_ORDER registration · improve-note text · per-build state reset · observe tag · meta read-back)
AST	functions 177 → 183 (+6: `_env_w52_timing_mode`, `_env_w52_low_pct`, `_env_w52_high_pct`, `_env_shock_pct`, `_w52_eval`, `_timing_gate`; 0 removed) · `py_compile` PASS · 0 new non-ASCII, 0 smart quotes
S2 — Design (WHY on the file header)
Evidence: Monitoring Sheet #5 — RDN.US −6.53 % on 09-30 to 0.14 % of its 52W range and still rank 1; MRP.US seated at 0.62 %; 44 names seated since 09-01 average −4.32 % from first seat. No gate read the 52W position or the last session's move; the 09-12 post-mortem rule was never built.
New gate "Timing (52W)" appended in `evaluate_gates` immediately before "Portfolio" (structural gate stays last); registered in `GATE_ORDER` at that true position (the v1.0.7 lesson — `first_failed_gate` sorts it after Sector Trend, before Portfolio).
Inputs: 52W position recomputed `(price − low) / (high − low) × 100` from the row's price / 52W High / 52W Low (fallback: the sheet's "52W Position %", percent points — the engine contract, verified on the 10-01 export: RDN 0.143, NVDA 88.709, DDI 97.925 reproduce); last-session move from "Percent Change" (FRACTION contract → ×100; |v| ≥ 1.5 read as percent). Unknown passes (News precedent).
Fail when `pos < TFB_T10_W52_LOW_PCT` (15) or `pos > TFB_T10_W52_HIGH_PCT` (85) or `move ≤ TFB_T10_SHOCK_PCT` (−5.0; a non-negative value disables the shock leg). Fail class NON_CRITICAL → WATCH (never DO_NOT_INVEST; a WATCH row is never seated or sized).
Gate env `TFB_T10_W52_TIMING` = off (default) | observe | enforce, read per call (no restart): off → nothing appended, cand dict / gates / payload byte-identical; observe → gate appended as PASSED with a note, ONE countable `[w52-observe] …` tag per audit row in `failure_reason` (the F-1b seam, after selection) and `meta["timing_gate"]` counters; enforce → gate fails, near-miss "How To Qualify" reads "Timing: wait until the price sits inside the 52W window and the last session was not a shock day …".
The four timing fields attach to `cand` ONLY when armed (v1.13.0 lineage precedent) — OFF cand dict byte-identical.
Scope cuts (register): upside shock not vetoed; ADD timing on held positions = portfolio_actions P-183; window/shock parameters are env (Saturday-sitting values).
S4 — Audits (real module, no stand-ins)
Battery	Result
New `tests/test_ob_w52_timing_p181.py` (sha `e1eb21c015b4b5ca…`) — W1 helpers on verbatim 10-01 values · W2 OFF == base dual-tree (`OB_BASE`) · W3 observe (seats/KPIs/near-miss identical to OFF; exactly one tag per row; counters) · W4 enforce (RDN/MRP/CIE → WATCH low, NVDA/DDI → WATCH high; never seated; untouched rows keep their OFF verdict; near-miss text) · W5 GATE_ORDER · W6 env hygiene · W7 idempotence · R1–R4 on the REAL Global_Markets page (6,609 rows, frozen clock)	57/57 PASS ×3, digest `4862fc4b9e3ccdae` ×3 (plain repo form without base/page: 51 PASS)
Real-page read-back (observe, 6,609 rows)	evaluated 6,609 · would_fail 2,568 (fail_low 1,922 · fail_high 543 · fail_shock 224) · unknown 149 · OFF seats `[RDN.US, ZTO.US, VEL.US]` → enforce seats `[ZTO.US, 0257.HK, MTDR.US]` (builder-only run, no stability layer)
Real-page dual-tree	OFF payload digest `4352e56e8df1ccff` == base (frozen clock 2026-10-01 06:00Z)
Existing batteries	`test_opportunity_builder.py` + `test_ob_price_xcheck.py` + p171 + rel_cluster: 38 passed / 5 failed = base (identical failure set; the 5 are the pre-existing xcheck network/budget cases)
Re-pinned tests (delivered)	`tests/test_ob_nearmiss_text_p171.py` (version pin 1.22.2 → 1.23.0, sha `957c5b6ffdf39df0…`) · `tests/test_opportunity_builder_rel_cluster_tag.py` (version pin → 1.23.0, sha `1df2edd719e21d28…`)
S5 — Delivery (paths at convention)
`core/analysis/opportunity_builder.py` (OLD, rebuilt) · `tests/test_ob_w52_timing_p181.py` (NEW) · `tests/test_ob_nearmiss_text_p171.py` (OLD, re-pinned) · `tests/test_opportunity_builder_rel_cluster_tag.py` (OLD, re-pinned) · `docs/evidence/TFB_Commit_Sheet_opportunity_builder_v1.23.0_2026-10-01.md` (NEW)
S6 — Arming / read-back
Commit + Render Manual Deploy (batched with build #4 portfolio_actions v1.14.0 — one deploy, Saturday rule disclosed: this is the week's one engine deploy). Deploy proof: `/health pf_gates.opportunity_builder_version = "1.23.0"`.
Render ENV `TFB_T10_W52_TIMING=observe` (one ENV per evidence run; your call). Read-back = next cockpit run: every audit row carries one `[w52-observe]` tag; `meta.timing_gate` on the payload; expected on today's board: RDN WOULD_FAIL (low + shock), MRP WOULD_FAIL (low), NVDA WOULD_FAIL (high), PINE ok (47.8 %).
Enforce = Saturday 10-03 sitting (it re-seats the board: today's three FT seats would all be WATCH).
Rollback: env unset (behaviour = v1.22.2) or `git revert`.
