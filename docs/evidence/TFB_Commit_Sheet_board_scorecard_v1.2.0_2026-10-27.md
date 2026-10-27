# Commit Sheet — `scripts/score_selection_log.py` v1.2.0 [CHALLENGER] + `board_scorecard.yml` v1.2.0 + `ci.yml` 1.1.43 — 2026-10-27

**Scope:** program register A2, part 3 of 3 (part 1: PR #848, the challenger order in the backend; part 2: PR #849, the cockpit's `_Shadow_Rank_Log`). The nightly board scorecard now scores the two orders the cockpit logs — champion = the live order, challenger = the shadow order — on the names each ranks 1–5, the same way it scores board seats, and prints ONE verdict of the promotion rule that was fixed before any data. ENV-gated (`TFB_BOARD_SCORECARD_CHALLENGER`, default off, byte-identical off). The workflow arms the gate; it reads `NO_DATA` until #848 / #849 are merged and armed.
**Files:** `scripts/score_selection_log.py` (1.1.0 → 1.2.0; 1,854 → 2,279 lines; zero function or constant removals), `tests/test_board_scorecard_p182.py` (legs P12–P14, pins 24/24), `.github/workflows/board_scorecard.yml` (v1.1.0 → v1.2.0, own commit), `.github/workflows/ci.yml` (1.1.43: the harness joins the blocking lean lane), this sheet.

## S1 — WHY

- `claude/TFB_October_2026_Recommendation_Review.md` (27 Oct): the live order runs backwards (Engine ROI vs realised excess Spearman −0.38; ranks 1–3 beat the market 38 % vs 70 % for ranks 7–10).
- `claude/TFB_Self_Correcting_Recommendation_Program_2026-10-27.md` §4 fixed the promotion rule **before** any shadow data: primary metric = hit rate of the excess vs the sector / venue reference at +5 sessions on the names ranked 1–5 by each order, same days; promote only with ≥ 40 scored per order, ≥ 20 trading days, challenger hit ≥ champion + 10 points, mean ≥ champion, worst not lower by > 3 pp, churn not higher by > 20 %; 40 trading days without that → keep the champion.
- #848 computes both orders every sync; #849 logs them once per UTC day. Nothing scored them. The scorecard (L1) already scores board seats against daily closes; scoring the two logged orders with the same instrument gives "same days, same benchmark, same base-price rule" for both.

## S2 — WHAT (all of it only when `TFB_BOARD_SCORECARD_CHALLENGER=on`)

`scripts/score_selection_log.py`
- Docstring: dated v1.2.0 WHY / WHAT / GATE / ROLLBACK block prepended; v1.1.0, v1.0.0 and v0.1.0 blocks verbatim. `SCRIPT_VERSION = "1.2.0"`. Added lines ASCII only.
- Rule constants (§4, one place): `CHAL_TOP_N 5`, `CHAL_MIN_SCORED 40`, `CHAL_MIN_DAYS 20`, `CHAL_MAX_DAYS 40`, `CHAL_HIT_EDGE_PP 10`, `CHAL_WORST_TOL_PP 3`, `CHAL_CHURN_TOL 1.20`.
- `shadow_events(values)`: the FIRST event of each UTC Day (earliest `Logged At`), CHAMPION / CHALLENGER rows ranked 1–5 only (BOARD rows and ranks 6–10 ignored). `Day` read by `_day_key()` (Sheets serial, ISO, m/d/Y); an unreadable Day falls back to the UTC date of `Logged At` (Riyadh − 3 h). Missing tab / empty grid / missing column → no events.
- `build_episodes()` unchanged: each order's daily snapshots become episodes (open on the first day a name is in that order's top 5, `DROPPED` on the first day it is not).
- `score_shadow_episode()`: base = the venue's last close **before** the first session after the logged moment (the log carries no price by design), then the v1.1.0 `score_episode()` (vs SPUS and vs the `ref_symbol()` reference). No series → `NO_DATA`; no prior close → `NO_PRICE`.
- `challenger_compare()` / `challenger_verdict()`: per order — Trading Days (distinct logged UTC days Monday–Friday; a weekend log still yields episodes, never a day), Episodes, Scored +5, Hit % +5 (vs ref; vs SPUS when the episode has no reference), Mean / Worst +5 vs Ref, Entries / Day, Mean +5 vs SPUS — and one verdict row: `NO_DATA` / `INSUFFICIENT_DATA` / `CONTINUE` (names the failed conditions) / `PROMOTE_ELIGIBLE` ("every condition met - owner decides") / `KEEP_CHAMPION` (40 trading days reached without promotion).
- `run_scorecard()`: live → `_read_shadow_live()` (UNFORMATTED_VALUE + SERIAL_NUMBER, as `_Selection_Log`: the cockpit's `Logged At` / `Day` text is stored by Sheets as dates); offline → `--shadow-tsv`. The shadow top-5 names and their references join the ONE fetch after board + SPUS + refs (the `frm` date widens if needed); board price coverage is unchanged. A missing tab → `NO_DATA` silently; a failed read → one `WARN` line + `NO_DATA`; never raises.
- Outputs: CHALLENGER vs CHAMPION block in stdout, the MD artifact and the step summary; a fourth block in `board_scorecard_summary.csv`; in the tab below RANKING HEALTH (or CHURN) and above DETAIL; meta `| challenger <verdict>`; final stdout ` challenger=<verdict>`; write mode `_Run_Log` ` | challenger: <verdict> (days d, scored c/k)` and a `challenger` object in the details JSON.
- Untouched: DETAIL (board seats only), `_Selection_Log` and `_Shadow_Rank_Log` READ ONLY, the v0.1.0 ticket path, every v1.1.0 function (four signatures extended with optional keywords only).

## S3 — GATE / ARMING

| Env | Before | After | Effect |
|---|---|---|---|
| `TFB_BOARD_SCORECARD_CHALLENGER` | (absent) | `"on"` in the job env | CHALLENGER vs CHAMPION. Off / unset → every v1.1.0 output byte-identical (P12). |

Rationale: read-only and additive; with no `_Shadow_Rank_Log` tab yet the block reads `NO_DATA`, so arming now means the evaluation clock starts on the first logged weekday without another PR. The verdict is evidence for the owner; nothing switches automatically.

## S4 — Tests

- `python scripts/score_selection_log.py --selftest` → **SELFTEST 24/24** (21 + 3: #22 shadow parsing — first event per UTC day, ranks 1–5, BOARD / 6+ ignored, live serials, Day fallback across midnight, Mon–Fri count; #23 base = close before the first session, `NO_PRICE`, `NO_DATA`; #24 every verdict, each guardrail failing alone → `CONTINUE` then `KEEP_CHAMPION` at 40 days, compare rows).
- `tests/test_board_scorecard_p182.py` → **134 PASS / 0 FAIL** (P2 / P4 skip without the optional fixtures; the P13 header contract skips until 16_Decision_Top10 v1.17.0 is on the tree — checked against PR #849's file: 24 columns, equal). `pytest -q` → 1 passed (1.4 s).
  - **P12** golden negative vs the untouched v1.1.0 (`git show 53c8544e`), ranking unset / on × challenger unset / off: stdout, three artifacts, step summary, rectangle, write calls and `_Run_Log` row byte-identical (tag normalised); the shadow tab is never touched; the base ignores the gate.
  - **P13** gate on, a 24-column `_Shadow_Rank_Log` fixture: `PROMOTE_ELIGIBLE` / `CONTINUE (hit edge)` / `CONTINUE (churn)` / `KEEP_CHAMPION` / `INSUFFICIENT_DATA` / `NO_DATA` end to end; every order row re-derived independently (base close, +5 vs XLF / SPUS, hit, mean, worst, churn); later same-day events, ranks 6–10 and BOARD names never scored nor fetched; a Saturday log not a trading day; live (serials) == TSV == fetch path; the read is UNFORMATTED + SERIAL_NUMBER and the tab is never written; missing tab → silent `NO_DATA`, failed read → `WARN` + `NO_DATA`; additivity — on minus the block (and verdict suffixes) == off for the summary CSV, MD, rectangle and `_Run_Log`, DETAIL byte-identical.
  - **P14** selftest 21 → 24; base v1.1.0 21/21.
- Workflow / CI regressions (`test_ci_required_verdict`, `test_github_workflow_run_window`, `test_runner_pin_ubuntu2404`, `test_security_pins_20261013`, `test_track_flags_mapped`, `test_workflow_activation_observe`, `test_workflow_flag_mappings`, `test_flag_registry`, `test_verify_manifest_pins`, `test_activation_rollout_evidence_workflows`, `test_monitor_recovery_guidance`, `test_matrix_validation_scope`) → 300 passed, 4 skipped; `scripts/audit_repository_workflows.py` errors 0, warnings 0.
- `compileall` PASS. AST diff vs 53c8544e: 0 removed, 20 added names.

## S5 — Rollback
- `TFB_BOARD_SCORECARD_CHALLENGER: "off"` (or delete the line) → v1.1.0 outputs byte for byte, next run.
- Reverting the commits restores 53c8544e exactly; nothing else depends on this gate.

## S6 — Known limits
- "Scored" counts episodes (entries), as L1 scores seats. A stable top 5 opens few episodes, so 40 scored per order may need most of the 40-day window; if not reached the rule keeps the champion (inconclusive → champion), by design.
- Trading day = a UTC Monday–Friday with a logged event; Tadawul's Sunday session does not count (the INVEST pool is mostly US / global). Weekend logs still yield episodes.
- First event per UTC day only; later same-day order changes stay in the log but are not scored.
- The shadow order is a list, not a fill: its base is the close before the first session after the log moment, while board seats use the printed seat price. Champion and challenger share the same rule, so their comparison is fair; champion vs board figures can differ slightly.
- Reference = the v1.1.0 mapping (US sector ETF by Sector word; venue index otherwise; unmapped → SPUS). Yahoo gaps leave episodes unscored (not counted).

## S7 — Operator steps
1. Merge #848 and #849 (producers) and this PR (reader) — any order; this one only reads.
2. Arm the producers: Render `TFB_OPP_RANK_MODE=shadow` outside a sync window; paste 16_Decision_Top10 v1.17.0 and set Script Property `DT10_SHADOW_RANK_LOG=on`.
3. Read the nightly `_Board_Scorecard` (01:23 Riyadh): the CHALLENGER block shows `NO_DATA` until the first logged weekday, then `INSUFFICIENT_DATA` for at least 20 trading days. `_Run_Log` carries `| challenger: <verdict> (days d, scored c/k)`.
4. Only on `PROMOTE_ELIGIBLE`: owner decision; a separate PR moves `enforce` and the cockpit's display order.
