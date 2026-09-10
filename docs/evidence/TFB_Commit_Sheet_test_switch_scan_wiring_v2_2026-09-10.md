# TFB Commit Sheet — tests/test_switch_scan_wiring.py v2 (P-117)
Date: 2026-09-10 · Builder: Claude · Separate single-purpose commit (Build #2)

## Identity
| Item | Value |
|---|---|
| Register item | **P-117** (red-team MR-07, ACCEPTED against pinned source 2026-09-10) |
| Base | tests/test_switch_scan_wiring.py v1 (63 lines) @ HEAD `bff7d869…` — sha256 `73f1b22e8a48c3b7…` |
| Delivered | v2 — sha256 `ed365cb8a72b3e15…` (112 lines, 4 tests) |
| Module under test | core/analysis/portfolio_actions.py v1.10.0 — UNCHANGED by this commit |
| Destination | tests/test_switch_scan_wiring.py (replace) + docs/evidence/ this sheet |

## Defect closed (v1 → v2)
v1 L58–63 wrapped the promotion assertions in `if status == "pending_persistence":` — any other status passed silently; L27–32 accepted `"unavailable"` in the feature-off test, so a broken module could green the suite.

## v2 contract
1. `test_off_no_switch_key_strict` — status must be exactly `"ok"`; `"unavailable"` now FAILS.
2. `test_on_empty_and_stale` — unchanged honest-refusal coverage.
3. `test_eligibility_and_persistence_unconditional` — dependency precondition **asserted** (`advisor_switch_scan` callable, never skipped); scan 1 `pending_persistence` asserted FLAT with `persist_day == 1` and empty proposals; scan 2 `proposals` asserted FLAT with `persist_day == 2` and empty pending. Confirmation unit documented explicitly: the counter counts **scans** via confirm-redis, not calendar days.
4. `test_dependency_failure_is_failsoft_and_disclosed` — NEW: raising dependency must not break the PF build AND must surface as `error:*` in scan meta (fail-soft proven, not assumed).

## Audits
py_compile PASS · harness 4/4 PASSED × 3, timing-normalized output identical · combined run with test_fund_unit_sentry.py: 16/16 · fixtures (HOLD/FX/OK/FT/NO) carried verbatim from v1, zero removals of covered behavior.

## Residual (F15 backlog, unchanged)
Cache-freshness/restart-persistence, live candidate-feed shape, and per-proposal cash-feasibility cases remain F15 items — this commit closes the assertion-bypass defect only.
