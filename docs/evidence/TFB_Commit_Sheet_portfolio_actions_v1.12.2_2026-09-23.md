# TFB Commit Sheet — core/analysis/portfolio_actions.py v1.12.2 [P-165 ADD-CONFIRM FAIL-CLOSED]

Date: 2026-09-23 (Riyadh) · Register: P-165 (accepted from the external review of 2026-09-23, confirmed at HEAD by Claude) · Lane: Render backend (one Manual Deploy, NO new ENV) · Protocol: One-Pass (S1 live pin → S3 anchored edits count==1 → S4 dual-tree real-module harness ×3 → S5 full-file delivery)

## 1. Base pinned (S1)

| Item | Value |
|---|---|
| Base | `core/analysis/portfolio_actions.py` v1.12.1 at `main`, live-fetched twice, zero drift vs the 2026-09-21 delivery |
| Base SHA-256 | `6a378895143584eecb825d111be4f778179045340f779f6159996cce9a4b9aeb` (3,279 lines, 87 defs) |
| Harness deps at HEAD | `core/__init__.py` ec2bc149…, `core/analysis/__init__.py` b9ddd9ac…, `core/analysis/opportunity_builder.py` 529a0864… (v1.22.0) |

## 2. Delivered (S5)

| File | Repo path | SHA-256 | Size |
|---|---|---|---|
| `portfolio_actions.py` | `core/analysis/portfolio_actions.py` | `ac6e32ac80a500ff8005d9c0f0e28b5d2a55949385fbe090f6b216f4a882ba6e` | 3,357 lines (+83 / −5), 88 defs |
| `test_pf_add_confirm_failclosed_p165.py` | `tests/test_pf_add_confirm_failclosed_p165.py` | `d58da5e24d128c31741baeeda751bfaab19a5d0e284dedfc8ce3b8701aec14f6` | 242 lines |
| this sheet | `docs/evidence/TFB_Commit_Sheet_portfolio_actions_v1.12.2_2026-09-23.md` | — | — |

Six anchored edits, each asserted count==1; `py_compile` PASS; AST names 87 → 88 (+`_add_confirm_failclosed_enabled`, 0 removed); no smart quotes; non-ASCII delta +2 (two em dashes inside user-facing reason/alert strings, the file's existing convention); all prior WHY blocks and fixes carried verbatim.

## 3. Root (confirmed at HEAD L2009–2077 of v1.12.1)

`_apply_add_confirmation(symbol, action, reason, capped_from, controls)` closes with `except Exception: return action, reason, capped_from`. The `action` argument is the RAW verdict, so an exception anywhere inside the gate returns an UNCONFIRMED ADD as ADD and the funding pass sizes it — the only rule in the file whose error path UPGRADES. Live exposure today: nil (the 09-23 Portfolio_Decision run rendered DDI "ADD confirmed (day 2/2)" and CARE "pending (day 1/2)", so the gate ran clean; the try block holds dict/date/Redis calls whose helpers themselves never raise). Latent contract defect, closed before it fires (v1.12.1 class).

Correction to the morning adjudication, owned: the "memory-only branch advances on any new date" sub-claim is the documented `TFB_PF_CONFIRM_PERSIST=0` legacy path (v1.5.1 byte-identical restore); with persistence armed (default) `_persist` reads the env, not the Redis client, so the STRICT-consecutiveness branch is taken even when Redis is dead. Unreachable in production config — deliberately NOT changed.

## 4. Change (one kill-switch, default ON = fail-closed)

| # | Seam | Behaviour |
|---|---|---|
| (a) | outer `except Exception` | ADD verdict → `ACTION_HOLD`, reason `"ADD held fail-closed [confirm-failclosed:<ExcType>] — the confirmation gate raised; no funding this run, the confirmation clock is untouched; qualifying: <reason>"`, `capped_from=ACTION_ADD` (the suppressed-action contract the funding pass already honours); the symbol's store entry is NOT touched; one `[CONFIRM-FAILCLOSED v1.12.2]` WARNING. Any non-ADD verdict returns unchanged — never suppresses TRIM/EXIT/BLOCK. |
| (b) | `add_confirm_days` parse fallback | unparsable depth → `DEFAULT_CONTROLS["add_confirm_days"]` (confirmation required) instead of 0 (gate off). `make_controls` validates the panel value, so this seam is unreachable in production — closed for the contract. |
| (c) | alerts | new `add_confirmation_gate_error` (count of fail-closed rows, appended only when > 0); the row is excluded from `low_confidence_capped` so it is counted exactly once, like every other capped class; it does not carry "pending confirmation" so `add_confirmation_pending` stays truthful. |

Gate: `TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN=1|true|on|yes` restores v1.12.1 byte-identically (P-127/P-130 precedent: the OFF state IS the defect; operator veto available). No new production ENV is required.

## 5. Evidence (S4) — dual-tree real-module harness, separate processes (both trees share the `core` package name)

Battery `tests/test_pf_add_confirm_failclosed_p165.py` T1–T6:

| Tree | Result |
|---|---|
| delivered v1.12.2 | **7 passed ×3** (T3 parametrised ×2), with the real 09-23 My_Portfolio export as the T6 fixture |
| base v1.12.1 | **3 failed / 4 passed — golden negative**: T2 (exception on ADD returns ADD), T4 (unparsable depth = gate off), T6 (the gate raising funds two unconfirmed ADDs) |
| CI-shaped (no fixture) | 6 passed, 1 skipped (T6 skips unless `TFB_TEST_MP_TSV` is set) |

Script-mode JSON digest ×3 per tree: base `c7e1dc8a1f6b1276` identical ×3, delivered `ae5783b488ad36e5` identical ×3. Scenario comparison base vs delivered:

- **Kill-switch scenarios (6/6) byte-identical** — `clean_True`, `exc_add_True`, `exc_trim_True`, `bad_days_True`, `int_clean_True`, `int_raise_True`.
- **Clean default paths identical** — `clean_False` (day 1 pending, same-day frozen, yesterday-dated chain confirms day 2/2, non-ADD resets) and `int_clean_False`: `build_portfolio_actions` on the real 6 holdings under the live panel (cash 24,763.73 / target 10 / max pos 20 / max sector 30 / rel 70 / DQ 80 / Advisory, FX USD 3.7558) reproduces the live page — NAV 93,583, holdings 68,819, deployable 15,405, 6×HOLD, CARE + DDI "ADD pending confirmation (day 1/2)" (sandbox has no Redis chain), alerts low_data_coverage 1 / add_confirmation_pending 2 / engine_precedence_veto 1 — IDENTICAL in both trees.
- **The fix (differ only under an injected exception)**: `int_raise_False` — base emits CARE ADD + DDI ADD and the funding pass sizes **14,366 SAR** of unconfirmed adds (`adds_funded_sar 14366`, ADD 2 / HOLD 4); delivered emits HOLD ×6, both rows tagged `[confirm-failclosed:RuntimeError]` with capped_from ADD, `add_confirmation_gate_error 2`, `adds_funded_sar 0`, every other row (SBAC/YUM/5023.SR/CWBC) byte-identical to the clean run. `exc_trim_*` identical in both trees (TRIM never suppressed).

Harness discoveries kept: `capped_from` is emitted under `detail`, not at the row root; the reason renders at two payload sites (`action_reason` + `advisor_note`) — count tags per row; YUM carries capped_from=ADD via the §4.7 precedence veto, a distinct class the fail-closed logic must not touch (it does not).

## 6. Deploy + read-back

1. Commit the three files in ONE push (per-file SHA check on "done"; the 09-22 lesson: github.dev pastes landed in the wrong file twice).
2. One Render **Manual Deploy** (backend file). Proof = boot log binds `core.analysis.portfolio_actions v1.12.2`; `/health` versions.portfolio_actions 1.12.2.
3. Read-back = next Portfolio_Decision run: status line `actions v1.12.2`, DDI/CARE rows unchanged in text and numbers (byte-equivalent live behaviour), zero `add_confirmation_gate_error` alerts. A gate_error alert ever appearing = a real exception in the gate → the `[CONFIRM-FAILCLOSED]` line names it.

Rollback: `git revert`, or `TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN=1` on Render (call-time read, no restart) → v1.12.1 semantics byte-identically.

## 7. Deliberate cuts

- The memory-only lenient branch (kill-switch legacy path) untouched — documented v1.5.1 restore contract.
- No change to the confirmation depth, the Redis write-through, the strict-consecutiveness rule, or any other seam (`decide_action`, `_apply_deminimis`, `_apply_reduce_policy`, `_apply_drawdown_guard`, `_apply_f1_observe_tag` byte-untouched).
- GAS side untouched: the new reason text renders through the existing Advisor Note column; the new alert type renders through the existing alerts table.
