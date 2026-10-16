TFB Commit Sheet — scripts/run_shadow_scorer.py v1.9.4 [P-186 LASTSCORED MADE SAFE TO ARM]
Date: 2026-10-16 (Friday, evidence day) · Lane: GitHub/Python (shadow_scorer.yml) · Protocol: One-Pass · File status: OLD file, rebuilt (full-file delivery) · Nothing armed by this delivery

S0/S1 — Base pin
Item	Value
Base	`scripts/run_shadow_scorer.py` v1.9.3 at `origin/main` 11ef134 (2,959 lines) — live in the GitHub lane (`[S1-GATE v1.9.3]` 10-15 19:12Z)
Delivered	sha `bda34c789df3041d…`, 3,050 lines, `SCRIPT_VERSION = "1.9.4"`
Edits	13 anchored edits, count==1 each (header WHY + version · `ls_day_verdict` helper · lastscored denominator guard · observe probe slot · read-back pairable key · probe capture · `ls_day` token · empty-base fallback at base selection · fallback-aware denominator guard · fallback-aware read-back pairings (3) · `fallback=` token)
AST	functions 80 → 81 (+1 `ls_day_verdict`; 0 removed) · `py_compile` PASS · ruff E9/F63/F7/F82 clean · embedded `--selftest` 116/116 (unchanged battery)

S2 — Why (measured, not assumed)
GitHub Actions census of the last 25 scorer runs (09-21 → 10-15): 13 of 19 weekdays scored; criterion 1 moved 7/28 → 19/28; 0 `no-challenger` days since 09-14 (board `cands` 1–10 on all 40 board runs). All 5 exclusions were `fresh-floor` with `stale=- nopair=-`:
- 10-08 `fresh=4/7` and 10-15 `fresh=1/4`: every held leg fresh; the legacy floor divides yesterday's paired legs by TODAY's seat count (`_chal_frac`, :2052).
- 09-28, 10-05, 10-12 (Mondays): the scorer ran 6–7 h after its 15:20Z cron, crossed Riyadh midnight and read a stale board (`STALE`, `DRIFT`).
P-186 `lastscored` (v1.9.1, 10-01) fixes the arithmetic but was never armed (no mapping in shadow_scorer.yml; no `[S1-BASE` token in any run). Replaying it on the real `main()` found two latent defects that made arming unsafe:
(a) `[S1-BASE] mode=lastscored readback_error=KeyError` on every run — the pass-1 measurement read under lastscored has no `pairable` key.
(b) An EMPTY-CHALLENGER weekday was SCORED: the pairable-leg denominator replaced the 0.0 of the empty basket, so a board with no Gen2=YES seat scored at `fresh=1/1` and wrote CHALLENGER `''`/`{}` with a numeric return. That row became the permanent last-scored base with zero pairable legs; every later day read `fresh=0/N` and was excluded until rollback.

S3 — Fix
(a) the read-back reads the base row's pairable legs when the measurement lacks them (the same number the floor divides by).
(b) the pairable-leg denominator applies only when today's challenger is non-empty — an empty challenger stays `no-challenger` under every policy and never becomes a scored base.
(c) observe read-back gains ` ls_day=<scored | non-trading | excluded:<reason>>` from the pure `ls_day_verdict()`: what lastscored WOULD decide today, so one observe run answers the arming question.
(d) a lastscored base that stored NO price (a scored day whose seats were all new and unpriced, e.g. Saudi names on a US-only session) would pair nothing and stay the base forever; such a run pairs against the legacy rows for EVERY basket (same interval) and prints `fallback=empty-base` (found independently by the 10-16 S-1 lane review).
Workflow: `.github/workflows/shadow_scorer.yml` maps `TFB_S1_BASE_POLICY: ${{ vars.TFB_S1_BASE_POLICY || 'legacy' }}` — inert wiring (default = the script default, byte-identical), so arming is a repo-Variable flip at the sitting, not another PR.
UNTOUCHED: legacy output on every surface (byte-identical to v1.9.3 apart from the version token, proven L6); observe rows are legacy rows; count_scored_days, evaluate_s1, basket math, fresh floor, HISTORY_HEADER, constants.

S4 — Audits (real module, real main() on the in-memory sheet stub)
Battery	Result
New `tests/test_s1_lastscored_safety_v194.py`	L1 10-15 replay (legacy `fresh=1/4` excluded · lastscored `fresh=1/1` scored, `legs=1/1`, no readback_error · observe legacy rows + `ls_day=scored`) · L2 empty-challenger weekday under lastscored → `no-challenger`, no scored empty base · L3 next day scored against the 10-13 base (`fresh=3/3`), no lock · L4 observe on the empty day → legacy rows + `ls_day=excluded:no-challenger` · L5 `ls_day_verdict` precedence 10/10 · L7 priceless lastscored base → aligned legacy fallback (rows == legacy), `fallback=empty-base` · L6 dual-tree vs v1.9.3: legacy History / S1_Gate / _Run_Log byte-identical on both stubs; the base reproduces (b) (`empty day return=-0.0111 note='n=3'`) and the lock (`next day: readback_error, fresh-floor`) — RESULT PASS ×3, output digest `10880eea26c13c53` ×3
Mutation	revert (b) → L2 fails (`['2026-10-14','CHALLENGER','','{}',-0.0111,…]`); revert (a) → L1 fails (`readback_error=KeyError`); remove (d) → L7 fails (`DAY_EXCLUDED_INFRA … excluded` instead of the legacy rows)
Existing batteries (version tokens re-pinned to v1.9.4)	`test_s1_base_policy_p186.py`, `test_s1_day_key_p176.py`, `test_s1_criteria_v2_p204.py`, `test_s1_board_fresh_guard.py` + every test naming run_shadow_scorer/run_shadow_board/shadow_scorer.yml + `test_workflow_flag_mappings.py` (new `test_s1_base_policy_requires_an_operator_variable_and_defaults_legacy`): 181 passed · `audit_repository_workflows.py` errors=0
Manifest	`scripts/verify_deployment.py` pin run_shadow_scorer 1.9.3 → 1.9.4 (pin line only; `test_verify_manifest_pins.py` green)

S5 — Delivery
`scripts/run_shadow_scorer.py` (OLD, rebuilt) · `tests/test_s1_lastscored_safety_v194.py` (NEW) · `tests/test_s1_base_policy_p186.py`, `tests/test_s1_day_key_p176.py`, `tests/test_s1_criteria_v2_p204.py` (OLD, version tokens) · `scripts/verify_deployment.py` (pin) · `.github/workflows/shadow_scorer.yml` (inert mapping) · `tests/test_workflow_flag_mappings.py` (+1 test) · this sheet

S6 — Arming / read-back (owner, GitHub lane; NOT done by this delivery)
Merge → the next 15:20Z slot runs `[S1-GATE v1.9.4]` in legacy (identical numbers).
Step 1 (observe, decision-neutral): set the repository Variable `TFB_S1_BASE_POLICY=observe` (Settings → Secrets and variables → Actions → Variables). Read-back on the next weekday verdict / S1_Gate row 3 / `_Run_Log` JSON `base_policy`: `[S1-BASE v1.9.4] mode=observe … fresh_den legacy=N pairable=P ls_day=<…>`.
Step 2 (Saturday sitting, evidence-lane decision with a Register §5 boundary note): `observe` → `lastscored`. Read-back: `chal fresh=a/P` with P = pairable base legs, `[S1-BASE v1.9.4] mode=lastscored … legs=a/P` and no `readback_error`.
Rollback: delete the Variable (legacy) or `git revert`. Prior scored days stand as recorded.

S7 — Addendum (same PR): inert mappings for the other built S-1 switches
Why: the review found three built S-1 switches with no workflow mapping, so arming any of them needed a code commit and a review cycle. Two of the four October exclusions (10-05, 10-12) were Mondays whose base row was empty after a weekend board rotation. That is the case P-175 (board v1.5.0) was built for.
What: `shadow_board.yml` maps `TFB_BOARD_FREEZE_NONTRADING` (`|| 'off'`) and `TFB_BOARD_FREEZE_HOLIDAYS` (`|| ''`). `shadow_scorer.yml` maps `TFB_S1_WINDOW_START` (`|| ''`, which means the script's own default of 2026-09-16). Each default equals the reader's own default, so behaviour is byte-identical while the Variables are unset. `tests/test_workflow_flag_mappings.py` gets 3 new mapping assertions, and the five S-1 names are added to the no-leak set (now also checked against `shadow_board.yml`). `TFB_S1_ZERO_BASELINE` (tracker lanes) is not mapped here; it belongs with the tracker PR.
Arming (owner, Saturday sitting; nothing armed here): `TFB_BOARD_FREEZE_NONTRADING=observe`. Read-back on the next weekend board: `[SHADOW-BOARD-FREEZE v1.5.0] would freeze (observe): weekend:Sat - run proceeds`. Then `enforce`: `frozen: weekend:Sat - board kept, nothing written`, and Monday pairs against the Friday base. Rollback: delete the Variable.
