# TFB Commit Sheet — L5: Signal_History dedup enforce + publication identity (track_performance v6.47.0) and the "alpha since" S1_Gate row (run_shadow_scorer v1.10.1)

Date: 2026-10-17 (Saturday) · Brief: Codex Brief 2026-10-17 v2 §2 **L5** (both bullets) · Arming is Emad's (§8: `TRACK_DEDUP_SIGNAL_HISTORY=enforce` after the measure read-back; `TFB_S1_ALPHA_SINCE=2026-08-31`)

## S0/S1 — Base pin
Branch `claude/new-session-twoifp` on `9f46633` (DT10 v1.14.0) ← … ← main `11ef134`.

| File | Base | Delivered | Change |
|---|---|---|---|
| `scripts/track_performance.py` | sha256 `fe690a00ca6ab7dbe330f5ec7d1f7494ad1cc7f9d49e82ba059aed3f9bec532b` (v6.46.0) | sha256 `5fc8692a007a4626d7bb58ebb9569070dc5f2fa9d73b204eb96494335a2267fa` (v6.47.0) | **+170 / −12** (8 anchored edits); removed lines re-emitted inside `load_key_rows` / the enforce branch / the token / selftest case 22 |
| `scripts/run_shadow_scorer.py` | sha256 `fde0a38da5d258c502cf348064d8c3ecc5d92c815b9ecff92eff687a89de299c` (v1.10.0) | sha256 `e43303347d1f8aa3fce368de86cd24b14c51fb98363aee9bdeeccc1f324efee6` (v1.10.1) | **+51 / −1** (5 anchored edits) |
| `scripts/workflow_activation_receipt.py` | sha256 `c94aae9f5cbff3aca06af8d82378e6377147a475f97d67e833db49a204201c86` | sha256 `44bf3760b2948e53f2630d48b683d79bfaac9753af2ff3ca8806dd227dbbaabd` | +7 / −1: `signal_history_apply`, and the two hard-coded booleans now follow the applied line |
| `scripts/verify_deployment.py` | sha256 `5a99bcde0edd0226c525aa3b0eaf141ae4300b2ac5afa8492b188069285889f4` | sha256 `1d2923a984c2d948867a82d307c9dfd09e6080e4ec73ad5f2dfd43fcf9905844` | +4 / −2: enforce reads as ARMED; row text |
| `.github/workflows/track_performance.yml` · `daily_sync.yml` | — | — | +5 / +5 comment lines only (dated note under the ROLLBACK line in both tracker steps; mapping lines untouched, ENV parity tests pass) |
| `tests/test_tp_dedup_signal_p191.py` | sha256 `3455cc5130f1355fe697e53cd0c9b040f3d238f47aa7175f06df1f57dc869264` | sha256 `ec6ad73b8162babb6676ae6e83fc71eaeef931a1464b553ef2a80178ed8c45a7` | +27 / −8: P4/P5 re-pinned to the new enforce contract (the v6.44.0 "measure-only" contract they pinned is the one this change replaces); fake WS gains `update()` |
| `tests/test_tp_dedup_signal_enforce_l5.py` | — | sha256 `2b8ba7fb8f0378835e6b07c3c4da6e5f6ef03d03d096fe4fb82cf32152531d03` | NEW, 10 tests |
| `tests/test_s1_alpha_since_l5.py` | — | sha256 `833a9e8b8afcfa9eb8f419f65224c8e77df7f6cf11310f22f9b48e47e51a0205` | NEW, 3 tests (real `main()` on the stub sheet) |
| 4 scorer test files · `tests/test_verify_optional_rollout_flags.py` · `tests/test_backtest_history_depth_l2.py` | — | — | version-token literals v1.10.0 → v1.10.1 and 120 → 121; verifier `armed` for enforce; L2 pins made version-relative |
| `.github/workflows/ci.yml` | — | — | +2 lean-list lines |

## S2 — Why
Signal_History: 31,168 rows, 8,281 unique symbol-day keys, **22,887 duplicates (73.4%)**, max 19 per key; `TRACK_DEDUP_SIGNAL_HISTORY=enforce` was parsed but measure-only since v6.44.0 (policy last|first left to the owner); the three publication-identity columns are blank on every row, so training cohorts can only key on symbol-day. Separately, the S-1 net alpha (+7.11%) was all earned before 31 Aug (since then −0.66% vs +0.30%) and S1_Gate shows only the cumulative window.

## S3 — What
**Tracker (enforce only; off/observe byte-identical to v6.46.0):**
- The brief's rule — one snapshot per symbol per Riyadh day, **keep the latest, never rewrite history** — is implemented as an **upsert of today's row only**: the key embeds the Riyadh day, so an existing key can only be today's row; its LAST row (keep-the-latest among the backlog) is overwritten in place, new keys append. Prior days are byte-untouched; the backlog stops growing from the first enforce run. The historical compaction (21.7k rows) remains a separate owner item.
- `SignalHistoryStore.load_key_rows` (one bounded read, positions kept across blank rows; `load_key_column` delegates), `upsert_snapshots(snaps, key_rows)` (in-batch last wins; `ws.update` of `A{r}:U{r}`; append with the capacity guard; `None` on any failure → the caller **falls back to the plain append**, i.e. the v6.46.0 behaviour; never under `TRACK_SIGNAL_PUBLICATION_MODE=immutable`, which owns identity).
- **Publication identity on new snapshots under enforce**: when `TRACK_PUBLICATION_ID` is set (both lanes map `github.run_id`) Publication Key / ID / Schema are filled with the v6.45.0 formulas; unset → blank, never guessed.
- Read-back: `_LAST_DEDUP_SIGNAL` gains `applied=True, policy, updated, appended`; `[PERF-VERDICT]` token ` | dedup_signal enforce(last-of-day) updated u appended a backlog B`; job log `[v6.47.0 DEDUP-SIGNAL] enforce applied last-of-day: updated=u appended=a …`; the tracker receipt reports `same_day_deduplication_applied=True`, `signal_history_measurement_only=False`, `signal_history_apply={policy, updated, appended}`; `verify_deployment` reads enforce as ARMED.
**Scorer (dark):** `TFB_S1_ALPHA_SINCE=YYYY-MM-DD` adds one informational S1_Gate meta row — challenger / champion / benchmark cum return since that date (`window_cum`: last index strictly before the date is the base) and the net alpha — labelled INFORMATIONAL ONLY; criteria, Shadow_History and the verdict line byte-identical; unset → no row. Selftest 120 → 121.

## S4 — Audits
| Check | Result |
|---|---|
| Anchored edits (8 + 5 + 2 + 2 + 2); `ast.parse`; YAML parse | PASS |
| Tracker suites: P-191 battery (re-pinned P4/P5), new enforce tests (positions across blank rows; today's last row updated — `A4:U4`/`A5:U5` — and only the new key appended; yesterday's rows never written; a later run overwrites today's row; the pure upsert keeps the last duplicate; identity filled under enforce with an id, blank without, never under observe; read failure → plain append + `read-failed` token; apply failure → plain append + `enforce would_skip` token; immutable path untouched; off/observe unchanged; receipt fields), canonical publications, activation observe (receipt booleans still False/True under observe), audit lane, record-only, flag mapping ×2, verifier flags, L2 depth, P-189b, P-158, P-201, P-180 | **245 passed** |
| Scorer suites: new alpha-since (reader; unset byte-identical vs set adds exactly one row with `window_cum` figures and an identical criteria block; pre-history date → `n/a`; junk → nothing), T6 replay, P-186, board-fresh guard, P-204, P-176, regret read failure, shape guard, calibration freshness, P-201 | **96 passed**; `--selftest` 121/121 |
| ruff on the 6 touched Python files | PASS |

## S5 — Delivery
`scripts/track_performance.py` · `scripts/run_shadow_scorer.py` · `scripts/workflow_activation_receipt.py` · `scripts/verify_deployment.py` · both tracker workflow files (comments) · tests as above · `ci.yml` · this sheet. No Variable / ENV change.

## S6 — Arming / read-back (Emad, §8)
1. Merge: nothing changes (observe stays the mapped default; the token text under observe is unchanged).
2. `TRACK_DEDUP_SIGNAL_HISTORY=enforce` (repository Variable, both lanes already map it): next tracker run → job log `enforce applied last-of-day: updated=u appended=a`, `[PERF-VERDICT] … dedup_signal enforce(last-of-day) …`, Signal_History grows by `appended` only, Publication Key/ID/Schema non-blank on the new rows; the following run's measure shows `backlog` flat. Rollback: `observe`.
3. `TFB_S1_ALPHA_SINCE=2026-08-31` on shadow_scorer.yml (map like `TFB_S1_CRITERIA_V2`): next scorer run → S1_Gate gains the `alpha since 2026-08-31:` row showing the three windows beside the cumulative line. Rollback: unset.

## Known limits
- "Keep the latest" on an append-only tab is only possible as an in-place update of today's row; the brief's "never rewrite history" is read as *prior days*. If the owner prefers first-of-day skip (zero in-place writes), `upsert_snapshots` reduces to the append branch — a one-line policy.
- Within a single build the builder still keeps the FIRST snapshot per key (its own dedup); last-of-day applies across runs.
- The 21.7k-row backlog is not compacted (owner item; G7 design note).
