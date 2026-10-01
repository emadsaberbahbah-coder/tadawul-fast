TFB Commit Sheet — core/analysis/portfolio_actions.py v1.14.0 [P-183 ADD-ON-LOSER VETO]
Date: 2026-10-01 (Thursday) · Lane: Render/Python · Build #3 of the day · Protocol: One-Pass · File status: OLD file, rebuilt (full-file delivery)
S0/S1 — Base pin
Item	Value
Base	`core/analysis/portfolio_actions.py` v1.13.1 at HEAD `7575c7d` (sha `1b88905a0194b3e0…`, 3,714 lines) — live per `/health` (`portfolio_actions_version 1.13.1`)
Delivered	sha `24fde87aea356472…`, 3,866 lines, `PORTFOLIO_ACTIONS_VERSION = "1.14.0"`
Edits	5 anchored edits, count==1 each (header+version · helpers · `_build` seam after the dd guard · alerts (+2, exclusion in low_confidence_capped) · meta read-back)
AST	functions 100 → 105 (+5: `_env_add_loser_veto_mode`, `_env_add_loser_pct`, `_env_add_stop_prox_pct`, `_add_loser_eval`, `_apply_add_loser_veto`; 0 removed) · `py_compile` PASS · 0 net-new non-ASCII
S2 — Design (WHY on the file header)
Evidence: `_Portfolio_Action_Log` — "ADD 58 sh CARE.US @ 30.07" on 09-25, four days before the stop-out at 29.55 (−723 SAR); today "ADD 20 sh AER.US ≈ 10,800 SAR, confirmed 2/2" on a position −3.3 % below cost. The ladder qualified ADDs on upside / reliability / DQ / headroom alone.
Post-decision seam (F-2 / F-1a pattern; `decide_action` byte-identical). Narrows ADD → HOLD when (a) `pnl_sar / cost_sar ≤ −TFB_PF_ADD_LOSER_PCT` (2.0 %) or (b) price within `TFB_PF_ADD_STOP_PROX_PCT` (2.0 %) above the ladder stop, or at/below it. Never upgrades; never touches HOLD/TRIM/EXIT/BLOCK; sukuk exempt (D-9); missing basis → pass-through.
Seam runs AFTER the confirmation clock: a vetoed ADD keeps its confirmed state and re-emerges the day the trigger clears.
`TFB_PF_ADD_LOSER_VETO` = off (default, v1.13.1 byte-identical) | observe | enforce, read per call. observe → one `[addveto-observe] …` tag (both render sites) + alert `add_loser_observe` + `meta.add_loser_veto`; enforce → HOLD "ADD vetoed [P-183]: … (was ADD: …)", proceeds 0, `capped_from=ADD`, alert `add_loser_veto`, excluded from `low_confidence_capped`.
Scope cuts: vol-scaled loser threshold later; stop = ladder stop, not the broker order; new-seat timing = opportunity_builder v1.23.0 (P-181).
S4 — Audits (real module; real 2026-10-01 My_Portfolio export, 6 holdings)
Battery	Result
New `tests/test_pf_add_loser_veto_p183.py` (sha `98ddf4695c9cde6f…`) — L1 helpers · L2 OFF == base dual-tree byte-for-byte; env inert on base · L3 observe (verdicts/KPIs/funding identical; AER tagged "position −3.3 % vs cost … would HOLD"; DDI "ok"; alert/meta) · L4 enforce: AER → HOLD, capped_from=ADD, delta 0; DDI keeps ADD + funding; adds_funded drops by AER's ticket; other 4 rows byte-identical · L5 hand fixtures (loser, near-stop, sukuk exempt, thresholds tunable, band 0 disables) · L6 hygiene · L7 idempotence	48/48 PASS ×3, digest `b4510c9ed7787b61` ×3 (plain repo form: 20 PASS)
Existing batteries	`test_portfolio_actions.py` + `test_pf_dd_guard_sukuk_exempt.py` + `test_pf_confirm_session_p168b.py` + `test_pf_sukuk_ladder_p153.py` (re-pinned): 21 passed, 1 skipped = base. The P-153 export-anchored S3 golden (09-30 CWBC 92.23) fails identically on base with today's export — fixture vintage, not a regression.
S5 — Delivery
`core/analysis/portfolio_actions.py` (OLD, rebuilt) · `tests/test_pf_add_loser_veto_p183.py` (NEW) · `tests/test_pf_sukuk_ladder_p153.py` (OLD, re-pinned 1.13.1 → 1.14.0, sha `99c9ccc722f59642…`) · `docs/evidence/TFB_Commit_Sheet_portfolio_actions_v1.14.0_2026-10-01.md` (NEW)
S6 — Arming / read-back
Commit + ONE Render Manual Deploy together with opportunity_builder v1.23.0. Proof: `/health pf_gates.portfolio_actions_version = "1.14.0"`.
Render ENV `TFB_PF_ADD_LOSER_VETO=observe` (separate evidence run from `TFB_T10_W52_TIMING`). Read-back: AER row "[addveto-observe] position −3.x% vs cost … would HOLD", alert add_loser_observe = 1.
Enforce = Saturday 10-03 sitting. Rollback: env unset or revert.
